import arcpy
import shutil
import os
import zipfile
import csv
from time import clock
from datetime import datetime, date, timedelta
# from hashlib import md5
from xxhash import xxh64
import json
import ntpath
import argparse
import re

import spec_manager
from oauth2client import tools
import driver

api_secrets = driver.SERVICE_ACCOUNT_SECRET_FILE
api_oauth = False
# If service account key file does not exist use a user account and OAuth2 instead.
if not os.path.exists(api_secrets):
    api_secrets = driver.OAUTH_CLIENT_SECRET_FILE
    api_oauth = True
# Declare all services and scopes required.
api_services = driver.ApiService((driver.APIS.drive, driver.APIS.sheets),
                                 secrets=api_secrets,
                                 scopes=' '.join((driver.AgrcDriver.FULL_SCOPE, driver.AgrcSheets.FULL_SCOPE)),
                                 use_oauth=api_oauth)
drive = driver.AgrcDriver(api_services.services[0])
sheets = driver.AgrcSheets(api_services.services[1])
user_drive = None
# If main drive service is a user account use it for file creation as well
if api_secrets == driver.OAUTH_CLIENT_SECRET_FILE:
    user_drive = drive

#IDs for drive objects
HASH_DRIVE_FOLDER = '0ByStJjVZ7c7mMVRpZjlVdVZ5Y0E'
UTM_DRIVE_FOLDER = '0ByStJjVZ7c7mNlZRd2ZYOUdyX2M'
LOG_SHEET_ID = '11ASS7LnxgpnD0jN4utzklREgMf1pcvYjcXcIcESHweQ'
LOG_SHEET_NAME = 'Drive Update'


def get_user_drive(user_drive=user_drive):
    """
    Get Drive service that has been authenticated as a user.

    It is important not to use a service account to create things because then it will be the owner
    and it is not in a domain.
    """
    if user_drive is None:
        user_services = driver.ApiService((driver.APIS.drive, driver.APIS.sheets),
                                          secrets=driver.OAUTH_CLIENT_SECRET_FILE,
                                          scopes=' '.join((driver.AgrcDriver.FULL_SCOPE,
                                                           driver.AgrcSheets.FULL_SCOPE)),
                                          use_oauth=True)
        user_drive = driver.AgrcDriver(user_services.services[0])
        return user_drive
    else:
        return user_drive


def _filter_fields(fields):
    """
    Filter out fields that mess up the change detection logic.

    fields: String[]
    source_primary_key: string
    returns: String[]
    """
    new_fields = [field for field in fields if not _is_naughty_field(field)]
    new_fields.sort()

    return new_fields


def _is_naughty_field(fld):
    #: global id's do not export to file geodatabase
    #: removes shape, shape_length etc
    #: removes objectid_ which is created by geoprocessing tasks and wouldn't be in destination source
    return fld.upper().startswith('SHAPE') or fld.upper().startswith('SHAPE_') or fld.startswith('OBJECTID')


def zip_folder(folder_path, zip_name):
    """Zip a folder with compression to reduce storage size."""
    zf = zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED)
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if not filename.endswith('.lock'):
                zf.write(os.path.join(root, filename),
                         os.path.relpath(os.path.join(root, filename), os.path.join(folder_path, '..')))
    original_size = 0
    compress_size = 0
    for info in zf.infolist():
        original_size += info.file_size
        compress_size += info.compress_size
    zf.close()


def unzip(zip_path, output_path):
    """Unzip a folder that was zipped by zip_folder."""
    with zipfile.ZipFile(zip_path, 'r', zipfile.ZIP_DEFLATED) as zipped:
        zipped.extractall(output_path)


def _get_copier(is_table):
    if is_table:
        return arcpy.CopyRows_management
    else:
        return arcpy.CopyFeatures_management


def create_outputs(output_directory, input_feature, output_name):
    """Create output file GDB and directory with shapefile."""
    # Create output GDB and feature class
    is_table = arcpy.Describe(input_feature).datasetType.lower() == 'table'
    copier = _get_copier(is_table)

    output_gdb = arcpy.CreateFileGDB_management(output_directory, output_name)[0]
    output_fc = copier(input_feature, os.path.join(output_gdb, output_name))[0]
    # Create directory to contain shape file
    shape_directory = os.path.join(output_directory, output_name)
    if not os.path.exists(shape_directory):
        os.makedirs(shape_directory)
    copier(output_fc, os.path.join(shape_directory, output_name))

    return (output_gdb, shape_directory)


def load_zip_to_drive(spec, id_key, new_zip, parent_folder_ids):
    """Create or update a zip file on drive."""
    # File should exist if id is in spec so use any account to update.
    if spec[id_key]:
        drive.update_file(spec[id_key], new_zip, 'application/zip')
    # File does not exist so create it with a user account in order to have control over ownership.
    else:
        temp_id = get_user_drive().create_drive_file(ntpath.basename(new_zip),
                                                     parent_folder_ids,
                                                     new_zip,
                                                     'application/zip')
        # Make agrc gmail account the owner
        get_user_drive().create_owner(temp_id, "agrc@utah.gov")
        spec[id_key] = temp_id

    # drive.keep_revision(spec[id_key])


def get_category_folder_id(category, parent_id):
    """Get drive id for a folder with name of category and in parent_id drive folder."""
    category_id = drive.get_file_id_by_name_and_directory(category, parent_id)
    if not category_id:
        print('Creating drive folder: {}'.format(category))
        category_id = get_user_drive().create_drive_folder(category, [parent_id])
        # Make agrc gmail account the owner
        get_user_drive().create_owner(category_id, "agrc@utah.gov")

    return category_id


def init_drive_package(package):
    """
    Create Drive folders for package and get Drive ids.

    package: package spec
    """
    category_id = get_category_folder_id(package['category'], UTM_DRIVE_FOLDER)
    category_packages_id = get_category_folder_id('packages', category_id)
    drive_folder_id = get_category_folder_id(package['name'], category_packages_id)
    gdb_folder_id = get_category_folder_id(package['name'] + '_gdb', drive_folder_id)
    shp_folder_id = get_category_folder_id(package['name'] + '_shp', drive_folder_id)
    if drive_folder_id not in package['parent_ids']:
        package['parent_ids'].append(drive_folder_id)
    if gdb_folder_id != package['gdb_id']:
        package['gdb_id'] = gdb_folder_id
    if shp_folder_id != package['shape_id']:
        package['shape_id'] = shp_folder_id
    spec_manager.save_spec_json(package)


def sync_package_and_features(package_spec):
    """Add package to features if it is not already there."""
    feature_list = [f.lower() for f in package_spec['feature_classes']]
    current_gdb_ids = []
    current_shp_ids = []

    for feature_spec in [spec_manager.get_feature(f) for f in feature_list]:
        package_list = [p.lower() for p in feature_spec['packages']]
        if package_spec['name'].lower() not in package_list:
            feature_spec['packages'].append(package_spec['name'])

        if package_spec['gdb_id'] not in drive.get_parents(feature_spec['gdb_id']):
            get_user_drive().add_file_parent(feature_spec['gdb_id'], package_spec['gdb_id'])
            print('add package gdb_id')
        if package_spec['shape_id'] not in drive.get_parents(feature_spec['shape_id']):
            get_user_drive().add_file_parent(feature_spec['shape_id'], package_spec['shape_id'])
            print('add package shape_id')

        current_gdb_ids.append(feature_spec['gdb_id'])
        current_shp_ids.append(feature_spec['shape_id'])

        spec_manager.save_spec_json(feature_spec)

    folder_gdb_ids = [name_id[1] for name_id in drive.list_files_in_directory(package_spec['gdb_id'])]
    for gdb_id in folder_gdb_ids:
        if gdb_id not in current_gdb_ids:
            get_user_drive().remove_file_parent(gdb_id, package_spec['gdb_id'])
            print('remove package gdb_id')

    folder_shp_ids = [name_id[1] for name_id in drive.list_files_in_directory(package_spec['shape_id'])]
    for shp_id in folder_shp_ids:
        if shp_id not in current_shp_ids:
            get_user_drive().remove_file_parent(shp_id, package_spec['shape_id'])
            print('remove package shp_id')


def sync_feature_to_package(feature_spec, package_spec):
    """Remove packages from feature if feature is not listed in package."""
    feature_list = [f.lower() for f in package_spec['feature_classes']]

    if feature_spec['sgid_name'].lower() not in feature_list:
        feature_spec['packages'].remove(package_spec['name'])
        if package_spec['gdb_id'] in drive.get_parents(feature_spec['gdb_id']):
            get_user_drive().remove_file_parent(feature_spec['gdb_id'], package_spec['gdb_id'])
            print('remove package gdb_id')
        if package_spec['shape_id'] in drive.get_parents(feature_spec['shape_id']):
            get_user_drive().remove_file_parent(feature_spec['shape_id'], package_spec['shape_id'])
            print('remove package shape_id')

    spec_manager.save_spec_json(feature_spec)


def src_data_exists(data_path):
    """Check for extistance and accessibility of data."""
    if not arcpy.Exists(data_path):
        return False
    try:
        with arcpy.da.SearchCursor(data_path, 'OID@'):
            pass
    except RuntimeError:
        return False

    return True


def update_feature(workspace, feature_name, output_directory, load_to_drive=True, force_update=False):
    """
    Update a feature class on drive if it has changed.

    workspace: string path or connection to a workspace that contains feature_name
    feature_name: string SGID name such as SGID.RECREATION.Trails
    """
    print('\nStarting feature:', feature_name)
    feature_time = clock()

    input_feature_path = os.path.join(workspace, feature_name)

    feature = spec_manager.get_feature(feature_name)
    if not src_data_exists(input_feature_path):
        now = datetime.now()
        log_sheet_values = [['{}.{}'.format(feature['category'], feature['name']),
                             now.strftime('%m/%d/%Y'),
                             now.strftime('%H:%M:%S.%f'),
                             clock() - feature_time]]
        sheets.append_row(LOG_SHEET_ID, LOG_SHEET_NAME, log_sheet_values)
        return []
    # Handle new packages and changes to feature['packages'] list
    for package in [spec_manager.get_package(p) for p in feature['packages']]:
        sync_feature_to_package(feature, package)

    category_id = get_category_folder_id(feature['category'], UTM_DRIVE_FOLDER)
    # Check for name folder
    name_id = get_category_folder_id(feature['name'], category_id)
    if name_id not in feature['parent_ids']:
        feature['parent_ids'].append(name_id)

    output_name = feature['name']

    packages = feature['packages']
    # Copy data local
    print('Copying...')
    fc_directory, shape_directory = create_outputs(
                                                    output_directory,
                                                    input_feature_path,
                                                    output_name)

    # Zip up outputs
    new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(output_name))
    new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(output_name))
    print('Zipping...')
    zip_folder(fc_directory, new_gdb_zip)
    zip_folder(shape_directory, new_shape_zip)
    # Upload to drive
    if load_to_drive:
        load_zip_to_drive(feature, 'gdb_id', new_gdb_zip, feature['parent_ids'])
        load_zip_to_drive(feature, 'shape_id', new_shape_zip, feature['parent_ids'])
        print('All zips loaded')

    spec_manager.save_spec_json(feature)
    now = datetime.now()
    log_sheet_values = [['{}.{}'.format(feature['category'], feature['name']),
                        now.strftime('%m/%d/%Y'),
                        now.strftime('%H:%M:%S.%f'),
                        clock() - feature_time]]
    sheets.append_row(LOG_SHEET_ID, LOG_SHEET_NAME, log_sheet_values)

    return packages


def get_changed_tables(workspace):
    change_detection_table = 'SGID.META.ChangeDetection'
    table_name = 'table_name'
    yesterday = date.today() - timedelta(days=1)
    query = f"last_modified = '{yesterday.strftime('%Y-%m-%d')}'"
    with arcpy.da.SearchCursor(os.path.join(workspace, change_detection_table), [table_name], where_clause=query) as cursor:
        return [f'sgid.{table}' for table, in cursor]


def run_features(workspace, output_directory, feature_list_json=None, load=True, force=False, category=None):
    """
    CLI option to update all features in spec_manager.FEATURE_SPEC_FOLDER or just those in feature_list_json.

    feature_list_json: json file with array named "features"
    """
    run_all_lists = None
    features = []
    if not feature_list_json:
        for feature_spec in spec_manager.get_feature_specs(get_changed_tables(workspace)):
            if feature_spec['sgid_name'] != '' and\
                    (category is None or category.upper() == feature_spec['category'].upper()):
                features.append(feature_spec['sgid_name'])
    else:
        with open(feature_list_json, 'r') as json_file:
            run_all_lists = json.load(json_file)
            features = run_all_lists['features']

    packages = []
    for feature in features:
        packages.extend(update_feature(workspace, feature, output_directory, load_to_drive=load, force_update=force))
    print('{} packages updated'.format(len(packages)))


def run_packages(workspace, output_directory, package_list_json=None, load=True, force=False):
    """
    CLI option to update all packages in spec_manager.PACKAGE_SPEC_FOLDER or just those in package_list_json.

    All features contianed in a package will also be updated if they have changed.
    package_list_json: json file with array named "packages"
    """
    run_all_lists = None
    features = []
    packages_to_check = []
    if not package_list_json:
        packages_to_check = spec_manager.get_package_specs(get_changed_tables(workspace))
    else:
        with open(package_list_json, 'r') as json_file:
            run_all_lists = json.load(json_file)
            for name in run_all_lists['packages']:
                packages_to_check.append(spec_manager.get_package(name))

    for package_spec in packages_to_check:

        if len(package_spec['parent_ids']) == 0 or package_spec['gdb_id'] == '' or package_spec['shape_id'] == '':
            init_drive_package(package_spec)
        sync_package_and_features(package_spec)

        fcs = package_spec['feature_classes']
        if fcs != '' and len(fcs) > 0:
            for f in fcs:
                if src_data_exists(os.path.join(workspace, f)):
                    features.append(f)
                else:
                    print('Package {}, feature {} does not exist'.format(package_spec, f))

    features = set(features)
    packages = []
    for feature in features:
        packages.extend(update_feature(workspace, feature, output_directory, load_to_drive=load, force_update=force))
    print('{} packages updated'.format(len(packages)))


def run_feature(workspace, source_name, output_directory, load=True, force=False):
    """CLI option to update one feature."""
    if src_data_exists(os.path.join(workspace, source_name)):
        packages = update_feature(workspace,
                                  source_name,
                                  output_directory,
                                  load_to_drive=load,
                                  force_update=force)
        for p in packages:
            print('Package updated: {}'.format(p))
    else:
        print('{} does not exist in workspace'.format(source_name))


def run_package(workspace, package_name, output_directory, load=True, force=False):
    """CLI option to update one feature."""
    temp_list_path = 'package_temp/temp_runlist_63717ac8.json'
    p_list = {'packages': [package_name]}
    with open(temp_list_path, 'w') as f_out:
        f_out.write(json.dumps(p_list, sort_keys=True, indent=4))
    run_packages(workspace,
                 output_directory,
                 temp_list_path,
                 load=load,
                 force=force)


def upload_zip(source_name, output_directory):
    """CLI option to upload zip files from update process run with load_to_drive=False."""
    feature = spec_manager.get_feature(source_name)
    output_name = feature['name']
    # Zip up outputs
    new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(output_name))
    new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(output_name))
    new_hash_zip = os.path.join(output_directory, '{}_hash.zip'.format(output_name))

    if not os.path.exists(new_gdb_zip) and \
       not os.path.exists(new_shape_zip) and \
       not os.path.exists(new_hash_zip):
        raise(Exception('Required zip file do not exist at {}'.format(output_directory)))

    # Upload to drive
    load_zip_to_drive(feature, 'gdb_id', new_gdb_zip, feature['parent_ids'])
    print('GDB loaded')
    load_zip_to_drive(feature, 'shape_id', new_shape_zip, feature['parent_ids'])
    print('Shape loaded')
    load_zip_to_drive(feature, 'hash_id', new_hash_zip, [HASH_DRIVE_FOLDER])
    print('Hash loaded')

    spec_manager.save_spec_json(feature)


def delete_feature(source_name):
    """Delete a feature, remove it from packages and delete all it's files on drive."""
    print('Deleting', source_name)
    confirm_delete = input('Are you sure you want to permanently delete files (yes, no): ')
    feature = spec_manager.get_feature(source_name)
    if 'y' not in confirm_delete.lower():
        print('Quitting without delete')
        return None

    drive_delete_user = get_user_drive()
    print('Deleting drive files')
    drive_delete_user.delete_file(feature['gdb_id'])
    drive_delete_user.delete_file(feature['hash_id'])
    drive_delete_user.delete_file(feature['shape_id'])
    drive_delete_user.delete_file(feature['parent_ids'][0])

    for package_name in feature['packages']:
        print('Deleting from package', package_name)
        spec_manager.remove_feature_from_package(package_name, feature['sgid_name'])

    print('Deleting json file')
    spec_manager.delete_spec_json(feature)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update zip files on drive', parents=[tools.argparser])

    parser.add_argument('-f', action='store_true', dest='force',
                        help='Force unchanged features and packages to create zip files')
    parser.add_argument('-n', action='store_false', dest='load',
                        help='Do not upload any files to drive')
    parser.add_argument('--all', action='store_true', dest='check_features',
                        help='Check all features for changes and update changed features and packages')
    parser.add_argument('--category', action='store', dest='feature_category',
                        help='Limits --all to specified category')

    parser.add_argument('--all_packages', action='store_true', dest='check_packages',
                        help='Update all packages that have changed features. Equivalent to --all with all features contained in package specs')
    parser.add_argument('--package_list', action='store', dest='package_list',
                        help='Check all packages in a json file with array named "packages".')
    parser.add_argument('--feature', action='store', dest='feature',
                        help='Check one feature for changes and update if needed. Takes one SGID feature name')
    parser.add_argument('--feature_list', action='store', dest='feature_list',
                        help='Check all features in a json file with array named "features".')
    parser.add_argument('--delete_feature', action='store', dest='delete_feature',
                        help='Delete feature from drive and json files.')
    parser.add_argument('--package', action='store', dest='package',
                        help='Check one package for changes and update if needed. Takes one package name')
    parser.add_argument('--upload_zip', action='store', dest='zip_feature',
                        help='Upload zip files for provided feature. Will fail if zip files do not exist in ./package_temp')
    parser.add_argument('workspace', action='store',
                        help='Set the workspace where all features are located')

    args = parser.parse_args()
    driver.flags = args  # flags global required for driver

    workspace = args.workspace #: SGID
    output_directory = r'package_temp'
    temp_package_directory = os.path.join(output_directory, 'output_packages')

    def renew_temp_directory(directory, package_dir):
        """Delete and recreate required temp directories."""
        if not os.path.exists(directory):
            os.makedirs(temp_package_directory)
        else:
            shutil.rmtree(directory)
            print('Temp directory removed')
            os.makedirs(package_dir)
    if not args.zip_feature:
        renew_temp_directory(output_directory, temp_package_directory)

    start_time = clock()

    if args.check_features:
        run_features(workspace,
                     output_directory,
                     load=args.load,
                     force=args.force,
                     category=args.feature_category)
    elif args.feature_list:
        run_features(workspace,
                     output_directory,
                     load=args.load,
                     force=args.force,
                     feature_list_json=args.feature_list)

    if args.check_packages:
        run_packages(workspace, output_directory, load=args.load, force=args.force)
    elif args.package_list:
        run_packages(workspace, output_directory, package_list_json=args.package_list, load=args.load, force=args.force)

    if args.feature:
        run_feature(workspace, args.feature, output_directory, load=args.load, force=args.force)

    if args.package:
        run_package(workspace, args.package, output_directory, load=args.load, force=args.force)

    if args.zip_feature:
        upload_zip(args.zip_feature, output_directory)

    if args.delete_feature:
        delete_feature(args.delete_feature)

    print('\nComplete!', clock() - start_time)

import arcpy
import shutil
import os
import zipfile
import csv
from time import clock, sleep
from datetime import datetime
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
    for root, subdirs, files in os.walk(folder_path):
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


def get_hash_lookup(hash_path, hash_field):
    """Get the has lookup for change detection."""
    hash_lookup = {}
    with arcpy.da.SearchCursor(hash_path, [hash_field]) as cursor:
        for row in cursor:
            hash_value = row[0]
            # hash_value, hash_oid = row
            if hash_value not in hash_lookup:
                hash_lookup[hash_value] = 0  # hash_oid isn't used for anything yet
            else:
                'Hash OID {} is duplicate wtf?'.format(hash_value)

    return hash_lookup


def detect_changes(data_path, fields, past_hashes, output_hashes, shape_token=None):
    """Detect any changes and create a new hash store for uploading."""
    float_subber = re.compile(r'(\d+\.\d{4})(\d+)')
    hash_store = output_hashes
    cursor_fields = list(fields)
    attribute_subindex = -1
    cursor_fields.append('OID@')
    if shape_token:
        cursor_fields.append('SHAPE@XY')  # TODO: Use centriod SHAPE@XY coordinates for change mapping.
        cursor_fields.append(shape_token)
        attribute_subindex = -3

    hashes = {}
    changes = 0
    with arcpy.da.SearchCursor(data_path, cursor_fields) as cursor, \
            open(hash_store, 'wb') as hash_csv:
            hash_writer = csv.writer(hash_csv)
            hash_writer.writerow(('hash',))
            # hash_writer.writerow(('src_id', 'hash', 'centroidxy'))
            for row in cursor:
                hasher = xxh64()  # Create/reset hash object
                hasher.update(float_subber.sub(r'\1', str(row[:attribute_subindex])))  # Hash only attributes
                if shape_token:
                    shape_string = row[-1]
                    if shape_string:  # None object won't hash
                        shape_string = float_subber.sub(r'\1', shape_string)
                        hasher.update(shape_string)
                    else:
                        hasher.update('No shape')  # Add something to the hash to represent None geometry object
                # Generate a unique hash if current row has duplicates
                digest = hasher.hexdigest()
                while digest in hashes:
                    hasher.update(digest)
                    digest = hasher.hexdigest()

                oid = row[attribute_subindex]
                hash_writer.writerow((digest,))
                # hash_writer.writerow((oid, digest, str(row[-2])))

                if digest not in past_hashes:
                    changes += 1

    print 'Total changes: {}'.format(changes)

    return changes


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

    drive.keep_revision(spec[id_key])


def get_category_folder_id(category, parent_id):
    """Get drive id for a folder with name of category and in parent_id drive folder."""
    category_id = drive.get_file_id_by_name_and_directory(category, parent_id)
    if not category_id:
        print 'Creating drive folder: {}'.format(category)
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
            print 'add package gdb_id'
        if package_spec['shape_id'] not in drive.get_parents(feature_spec['shape_id']):
            get_user_drive().add_file_parent(feature_spec['shape_id'], package_spec['shape_id'])
            print 'add package shape_id'

        current_gdb_ids.append(feature_spec['gdb_id'])
        current_shp_ids.append(feature_spec['shape_id'])

        spec_manager.save_spec_json(feature_spec)

    folder_gdb_ids = [name_id[1] for name_id in drive.list_files_in_directory(package_spec['gdb_id'])]
    for gdb_id in folder_gdb_ids:
        if gdb_id not in current_gdb_ids:
            get_user_drive().remove_file_parent(gdb_id, package_spec['gdb_id'])
            print 'remove package gdb_id'

    folder_shp_ids = [name_id[1] for name_id in drive.list_files_in_directory(package_spec['shape_id'])]
    for shp_id in folder_shp_ids:
        if shp_id not in current_shp_ids:
            get_user_drive().remove_file_parent(shp_id, package_spec['shape_id'])
            print 'remove package shp_id'


def sync_feature_to_package(feature_spec, package_spec):
    """Remove packages from feature if feature is not listed in package."""
    feature_list = [f.lower() for f in package_spec['feature_classes']]

    if feature_spec['sgid_name'].lower() not in feature_list:
        feature_spec['packages'].remove(package_spec['name'])
        if package_spec['gdb_id'] in drive.get_parents(feature_spec['gdb_id']):
            get_user_drive().remove_file_parent(feature_spec['gdb_id'], package_spec['gdb_id'])
            print 'remove package gdb_id'
        if package_spec['shape_id'] in drive.get_parents(feature_spec['shape_id']):
            get_user_drive().remove_file_parent(feature_spec['shape_id'], package_spec['shape_id'])
            print 'remove package shape_id'

    spec_manager.save_spec_json(feature_spec)


def src_data_exists(data_path):
    """Check for extistance and accessibility of data."""
    if not arcpy.Exists(data_path):
        return False
    try:
        with arcpy.da.SearchCursor(data_path, 'OID@') as cursor:
            pass
    except RuntimeError as e:
        return False

    return True


def update_feature(workspace, feature_name, output_directory, load_to_drive=True, force_update=False):
    """
    Update a feature class on drive if it has changed.

    workspace: string path or connection to a workspace that contains feature_name
    feature_name: string SGID name such as SGID10.RECREATION.Trails
    """
    print '\nStarting feature:', feature_name
    feature_time = clock()

    input_feature_path = os.path.join(workspace, feature_name)

    feature = spec_manager.get_feature(feature_name, create=True)
    if not src_data_exists(input_feature_path):
        now = datetime.now()
        log_sheet_values = [['{}.{}'.format(feature['category'], feature['name']),
                             'Does not exist',
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

    # Get the last hash from drive to check changes
    past_hash_directory = os.path.join(output_directory, 'pasthashes')
    hash_field = 'hash'
    past_hash_zip = os.path.join(output_directory, output_name + '_hash' + '.zip')
    past_hash_store = os.path.join(past_hash_directory, output_name + '_hash', output_name + '_hashes.csv')
    past_hashes = None
    if feature['hash_id']:
        drive.download_file(feature['hash_id'], past_hash_zip)
        print 'Past hashes downloaded'
        unzip(past_hash_zip, past_hash_directory)
        past_hashes = get_hash_lookup(past_hash_store, hash_field)
    else:
        past_hashes = {}

    # Check for changes
    # Create directory for feature hashes
    hash_directory = os.path.join(output_directory, output_name + '_hash')
    if not os.path.exists(hash_directory):
        os.makedirs(hash_directory)
    hash_store = os.path.join(hash_directory, '{}_hashes.csv'.format(output_name))
    # Get fields for hashing
    fields = set([fld.name for fld in arcpy.ListFields(input_feature_path)])
    fields = _filter_fields(fields)

    shape_token = None
    if not arcpy.Describe(input_feature_path).datasetType.lower() == 'table':
        shape_token = 'SHAPE@WKT'

    try:
        change_count = detect_changes(input_feature_path, fields, past_hashes, hash_store, shape_token)
    except RuntimeError:
        change_count = -1

    packages = []
    if change_count != 0 or force_update:
        packages = feature['packages']
        # Copy data local
        print 'Copying...'
        fc_directory, shape_directory = create_outputs(
                                                     output_directory,
                                                     input_feature_path,
                                                     output_name)

        # Zip up outputs
        new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(output_name))
        new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(output_name))
        new_hash_zip = os.path.join(output_directory, '{}_hash.zip'.format(output_name))
        print 'Zipping...'
        zip_folder(fc_directory, new_gdb_zip)
        zip_folder(shape_directory, new_shape_zip)
        zip_folder(hash_directory, new_hash_zip)
        # Upload to drive
        if load_to_drive:
            load_zip_to_drive(feature, 'gdb_id', new_gdb_zip, feature['parent_ids'])
            load_zip_to_drive(feature, 'shape_id', new_shape_zip, feature['parent_ids'])
            load_zip_to_drive(feature, 'hash_id', new_hash_zip, [HASH_DRIVE_FOLDER])
            print 'All zips loaded'

        spec_manager.save_spec_json(feature)
        now = datetime.now()
        if change_count == -1:
            change_count = 'Change detection error'
        log_sheet_values = [['{}.{}'.format(feature['category'], feature['name']),
                            change_count,
                            now.strftime('%m/%d/%Y'),
                            now.strftime('%H:%M:%S.%f'),
                            clock() - feature_time]]
        sheets.append_row(LOG_SHEET_ID, LOG_SHEET_NAME, log_sheet_values)

    return packages


def update_package(workspace, package_name, output_directory, load_to_drive=True, force_update=False):
    """Update a package on drive."""
    print '\nStarting package:', package_name
    package = spec_manager.get_package(package_name)
    # Check for category folder
    category_id = get_category_folder_id(package['category'], UTM_DRIVE_FOLDER)
    category_packages_id = get_category_folder_id('packages', category_id)
    drive_folder_id = get_category_folder_id(package['name'], category_packages_id)
    if drive_folder_id not in package['parent_ids']:
        package['parent_ids'].append(drive_folder_id)

    package_gdb = arcpy.CreateFileGDB_management(output_directory, package['name'])[0]
    package_shape = os.path.join(output_directory, package['name'])
    os.makedirs(package_shape)
    print 'Copying...'
    for feature_class in package['feature_classes']:
        spec_name = spec_manager.create_feature_spec_name(feature_class)
        feature_spec = os.path.join('features', spec_name)
        if not os.path.exists(feature_spec):
            print 'New feature'
            update_feature(workspace, feature_class, os.path.join(output_directory, '..'), load_to_drive, force_update)

        spec = spec_manager.get_feature(feature_class, [package_name], create=True)

        is_table = arcpy.Describe(os.path.join(workspace, feature_class)).datasetType.lower() == 'table'
        copier = _get_copier(is_table)

        feature_output_name = spec['name']
        out_fc_path = os.path.join(package_gdb, feature_output_name)

        shape_directory_path = os.path.join(output_directory, '..', feature_output_name)
        fc_path = os.path.join(shape_directory_path + '.gdb', feature_output_name)
        if os.path.exists(shape_directory_path) and arcpy.Exists(fc_path):
            # print feature_class, 'local'
            copier(fc_path,
                   out_fc_path)

            shutil.copytree(shape_directory_path, os.path.join(package_shape, feature_output_name))

        else:
            # print feature_class, 'workspace'
            copier(os.path.join(workspace, feature_class),
                   out_fc_path)

            s_dir = os.path.join(package_shape, feature_output_name)
            os.makedirs(s_dir)
            copier(os.path.join(workspace, feature_class),
                   os.path.join(s_dir, feature_output_name))

    # Zip up outputs
    new_gdb_zip = os.path.join(output_directory, '{}_gdb.zip'.format(package['name']))
    new_shape_zip = os.path.join(output_directory, '{}_shp.zip'.format(package['name']))
    print 'Zipping...'
    zip_folder(package_gdb, new_gdb_zip)
    zip_folder(package_shape, new_shape_zip)

    if load_to_drive:
        # Upload to drive
        load_zip_to_drive(package, 'gdb_id', new_gdb_zip, package['parent_ids'])
        load_zip_to_drive(package, 'shape_id', new_shape_zip, package['parent_ids'])
        print 'All zips loaded'

    spec_manager.save_spec_json(package)


def run_features(workspace, output_directory, feature_list_json=None, load=True, force=False, category=None, skip_packages=False, update_cycles=None):
    """
    CLI option to update all features in spec_manager.FEATURE_SPEC_FOLDER or just those in feature_list_json.

    feature_list_json: json file with array named "features"
    """
    run_all_lists = None
    features = []
    if not feature_list_json:
        for feature_spec in spec_manager.get_feature_specs(update_cycles):
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
    print '{} packages updated'.format(len(packages))


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
        packages_to_check = spec_manager.get_package_specs()
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
                    print 'Package {}, feature {} does not exist'.format(package_spec, f)

    features = set(features)
    packages = []
    for feature in features:
        packages.extend(update_feature(workspace, feature, output_directory, load_to_drive=load, force_update=force))
    print '{} packages updated'.format(len(packages))


def run_feature(workspace, source_name, output_directory, load=True, force=False, skip_packages=False):
    """CLI option to update one feature."""
    if src_data_exists(os.path.join(workspace, source_name)):
        packages = update_feature(workspace,
                                  source_name,
                                  output_directory,
                                  load_to_drive=load,
                                  force_update=force)
        for p in packages:
            print 'Package updated: {}'.format(p)
    else:
        print '{} does not exist in workspace'.format(source_name)
    # if not skip_packages:
    #     for package in set(packages):
    #         update_package(workspace, package, temp_package_directory, load_to_drive=load)


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
    print 'GDB loaded'
    load_zip_to_drive(feature, 'shape_id', new_shape_zip, feature['parent_ids'])
    print 'Shape loaded'
    load_zip_to_drive(feature, 'hash_id', new_hash_zip, [HASH_DRIVE_FOLDER])
    print 'Hash loaded'

    spec_manager.save_spec_json(feature)


def delete_feature(source_name):
    """Delete a feature, remove it from packages and delete all it's files on drive."""
    print 'Deleting', source_name
    confirm_delete = raw_input('Are you sure you want to permanently delete files (yes, no): ')
    feature = spec_manager.get_feature(source_name)
    if 'y' not in confirm_delete.lower():
        print 'Quitting without delete'
        return None

    drive_delete_user = get_user_drive()
    print 'Deleting drive files'
    drive_delete_user.delete_file(feature['gdb_id'])
    drive_delete_user.delete_file(feature['hash_id'])
    drive_delete_user.delete_file(feature['shape_id'])
    drive_delete_user.delete_file(feature['parent_ids'][0])

    for package_name in feature['packages']:
        print 'Deleting from package', package_name
        spec_manager.remove_feature_from_package(package_name, feature['sgid_name'])

    print 'Deleting json file'
    spec_manager.delete_spec_json(feature)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update zip files on drive', parents=[tools.argparser])

    parser.add_argument('-f', action='store_true', dest='force',
                        help='Force unchanged features and packages to create zip files')
    parser.add_argument('-n', action='store_false', dest='load',
                        help='Do not upload any files to drive')
    parser.add_argument('-s', action='store_true', dest='skip_packages',
                        help='Do not run packages from features that have changed')
    parser.add_argument('--all', action='store_true', dest='check_features',
                        help='Check all features for changes and update changed features and packages')
    parser.add_argument('--category', action='store', dest='feature_category',
                        help='Limits --all to specified category')
    # Update cycle to run
    parser.add_argument('-d', action='store_true', dest='daily',
                        help='Limits --all to daily updated features')
    parser.add_argument('-w', action='store_true', dest='weekly',
                        help='Limits --all to weekly updated features')
    parser.add_argument('-m', action='store_true', dest='monthly',
                        help='Limits --all to monthly updated features')
    parser.add_argument('-q', action='store_true', dest='quarterly',
                        help='Limits --all to quarterly updated features')
    parser.add_argument('-b', action='store_true', dest='biannual',
                        help='Limits --all to yearly updated features')
    parser.add_argument('-y', action='store_true', dest='yearly',
                        help='Limits --all to yearly updated features')

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

    workspace = args.workspace  # r'Database Connections\Connection to sgid.agrc.utah.gov.sde'
    output_directory = r'package_temp'
    temp_package_directory = os.path.join(output_directory, 'output_packages')

    def renew_temp_directory(directory, package_dir):
        """Delete and recreate required temp directories."""
        if not os.path.exists(directory):
            os.makedirs(temp_package_directory)
        else:
            shutil.rmtree(directory)
            print 'Temp directory removed'
            os.makedirs(package_dir)
    if not args.zip_feature:
        renew_temp_directory(output_directory, temp_package_directory)

    start_time = clock()

    if args.check_features:
        update_cycles = []
        if args.daily:
            update_cycles.append(spec_manager.UPDATE_CYCLES.DAY)
        if args.weekly:
            update_cycles.append(spec_manager.UPDATE_CYCLES.WEEK)
        if args.monthly:
            update_cycles.append(spec_manager.UPDATE_CYCLES.MONTH)
        if args.quarterly:
            update_cycles.append(spec_manager.UPDATE_CYCLES.QUARTER)
        if args.biannual:
            update_cycles.append(spec_manager.UPDATE_CYCLES.BIANNUAL)
        if args.yearly:
            update_cycles.append(spec_manager.UPDATE_CYCLES.ANNUAL)

        run_features(workspace,
                     output_directory,
                     load=args.load,
                     force=args.force,
                     category=args.feature_category,
                     skip_packages=args.skip_packages,
                     update_cycles=update_cycles)
    elif args.feature_list:
        run_features(workspace,
                     output_directory,
                     load=args.load,
                     force=args.force,
                     feature_list_json=args.feature_list,
                     skip_packages=args.skip_packages)

    if args.check_packages:
        run_packages(workspace, output_directory, load=args.load, force=args.force)
    elif args.package_list:
        run_packages(workspace, output_directory, package_list_json=args.package_list, load=args.load, force=args.force)

    if args.feature:
        run_feature(workspace, args.feature, output_directory, load=args.load, force=args.force, skip_packages=args.skip_packages)

    if args.package:
        run_package(workspace, args.package, output_directory, load=args.load, force=args.force)

    if args.zip_feature:
        upload_zip(args.zip_feature, output_directory)

    if args.delete_feature:
        delete_feature(args.delete_feature)

    print '\nComplete!', clock() - start_time

"""Junk module of functions that help update ftp links to drive links on gis.utah.gov"""
import os
import hashlib
import re
import json
import shutil
import csv
import time

import spec_manager
import driver
api_services = driver.ApiService((driver.APIS.drive, driver.APIS.sheets),
                                 secrets=driver.OAUTH_CLIENT_SECRET_FILE,
                                 scopes=' '.join((driver.AgrcDriver.FULL_SCOPE, driver.AgrcSheets.FULL_SCOPE)),
                                 use_oauth=True)
user_drive = driver.AgrcDriver(api_services.services[0])
user_sheets = driver.AgrcSheets(api_services.services[1])


class FtpLink(object):
    unique_links = {}

    def __init__(self, category, name, packaged, src_dir, ext, path):
        self.category = category
        self.name = name
        self.packaged = packaged
        self.src_dir = src_dir
        self.ext = ext
        self.path = path

        FtpLink.unique_links["{}:{}".format(self.category, self.name)] = 0

    def get_catname(self):
        catname = self.category + '_' + self.name
        return catname.lower()

    def __str__(self):
        return "cat: {}\nname: {}\npack: {}\nsrc: {}\next: {}".format(self.category,
                                                                      self.name,
                                                                      self.packaged,
                                                                      self.src_dir,
                                                                      self.ext)


def get_features_without_cycle():
    pass


def get_update_cycles(steward_info):
    update_types = set()
    with open(steward_info, 'rb') as info:
        reader = csv.DictReader(info)
        for row in reader:
            update_types.add(row['Refresh Cycle (Days)'].lower())

    update_types = list(update_types)
    update_types.sort()
    for t in update_types:
        print t


def get_feature_update_cycles(steward_info):
    features = {}
    with open(steward_info, 'rb') as info:
        reader = csv.DictReader(info)
        for row in reader:
            feature = 'SGID10.' + row['SGID Data Layer']
            if feature in features or feature.strip() == '':
                print 'wtf?'
                continue
            update = row['Refresh Cycle (Days)'].lower()
            features[feature] = update

    return features


def set_spec_update_types(feature_name, update_type):
    import arcpy
    if not arcpy.Exists(os.path.join(r'Database Connections\Connection to sgid.agrc.utah.gov.sde', feature_name)):
        print '^Does not exist'
        return
    feature = spec_manager.get_feature(feature_name)
    feature['update_cycle'] = update_type
    spec_manager.save_spec_json(feature)


def package_specs_from_gdbs(directory_path, category):
    import arcpy
    arcpy.env.workspace = directory_path
    gdbs = arcpy.ListWorkspaces('*', 'FileGDB')
    new_package_paths = []
    #
    for gdb in gdbs:
        arcpy.env.workspace = gdb
        gdb_name = os.path.basename(gdb).replace('.gdb', '')
        data = []
        data.extend(arcpy.ListFeatureClasses())
        data.extend(arcpy.ListTables())
        sgid_names = ['SGID10.{}.{}'.format(category.upper(), d) for d in data]
        package_json = spec_manager.create_package_spec(gdb_name,
                                                        sgid_names,
                                                        category.upper())
        print package_json
        print sgid_names
        print
        new_package_paths.append(package_json)

    spec_manager._list_packages_with_nonexistant_features(r'Database Connections\Connection to sgid.agrc.utah.gov.sde',
                                                          new_package_paths)


def get_directory_count(top_dir):
    # top_dir = r'/Volumes/ftp/UtahSGID_Vector'
    dircount = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in dirs:
            dir_path = os.path.join(root, name)
            print dir_path
            dircount += 1
    print dircount


def get_file_count(top_dir, ext, size_limit=None):
    file_count = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            if name.endswith(ext):
                if size_limit:
                    if os.path.getsize(dir_path) <= size_limit:
                        file_count += 1
                else:
                    file_count += 1

    print 'count: {}, type: {}, size <= {}'.format(file_count, ext, size_limit)


def hash_files(file_list):
    hex_digest = []
    for f in file_list:
            file_path = f
            local_file_hash = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
            hex_digest.append(local_file_hash)

    for h in hex_digest:
        print h


def get_all_ftp_links(top_dir):
    ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/SGID93_Vector/NAD83/MetadataHTML)(.+?)[\"\)]')
    # ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+?)[\"\)]')
    data_paths = []

    def get_ftp_link_in_file(path, matcher):
        data_links = []
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    data_links.extend([m[1] for m in matches])

        return data_links

    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = get_ftp_link_in_file(dir_path, ftp_link_matcher)
            data_paths.extend(links)

    return data_paths


def list_ftp_links_by_subfolder(top_dir):
    ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+?)[\"\)]')
    data_paths = []

    def get_ftp_link_in_file(path, matcher):
        data_links = []
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    data_links.extend([m[1] for m in matches])

        return data_links

    sub_dir_counts = {}
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for d in dirs:
            if root == top_dir:
                sub_dir_counts[d] = 0

    for sub_dir in sub_dir_counts:
        links = get_all_ftp_links(os.path.join(top_dir, sub_dir))
        print sub_dir, len(links)
        sub_dir_counts[sub_dir] = len(links)

    return sub_dir_counts


def replace_ftp_link(ftp_path, feature_specs, package_specs):

    def get_replace_link(link, spec):
        last_7 = link.path[-7:]
        zip_id = None
        if link.ext == '.zip' and last_7 in ['shp.zip', 'gdb.zip']:
            if 'shp' in last_7:
                if link.packaged:
                    zip_id = driver.get_webview_link(spec['shape_id'])
                    print 'package direct', link.path
                else:
                    zip_id = driver.get_download_link(spec['shape_id'])
            elif 'gdb' in last_7:
                if link.packaged:
                    zip_id = driver.get_webview_link(spec['gdb_id'])
                    print 'package direct', link.path
                else:
                    zip_id = driver.get_download_link(spec['gdb_id'])
            return '{}'.format(zip_id)
        elif link.ext is None:
            return '{}'.format(driver.get_webview_link(spec['parent_ids'][0]))

    ftp_link_matcher = re.compile(r'(ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+)')
    matches = ftp_link_matcher.findall(ftp_path)
    if len(matches) > 0:
        for m in matches:
            link = parse_ftp_link(m[1], '')
            if link is None:
                print 'other:', m[1]
                print ftp_path
            elif (not link.packaged and link.get_catname() in feature_specs):
                replace_link = get_replace_link(link, feature_specs[link.get_catname()])
                if replace_link is None:
                    print 'other:', link.path
                    continue
                return replace_link
            elif (link.packaged and link.get_catname() in package_specs):
                replace_link = get_replace_link(link, package_specs[link.get_catname()])
                if replace_link is None:
                    print 'other:', link.path
                    continue
                return replace_link
            else:
                print 'not found in specs:', link.path, ftp_path


def replace_metadata_links(top_dir='data/ftplinktest', rewrite_source=False):
    ftp_link_matcher = ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/SGID93_Vector/NAD83/MetadataHTML)(.+?)[\"\)]')
    data_paths = []
    not_founds = []

    def get_replace_link(link):
        new_prefix = 'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        new_html_file_path = '/Volumes/ftp/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        if not os.path.exists(new_html_file_path + 'SGID10.' + link + '.xml'):
            print link, 'does not exist as new'
            return None
        else:
            return new_prefix + 'SGID10.' + link + '.xml'

    def check_ftp_links_in_file(path, matcher, preview_name):
        data_links = []
        lines = []
        re_write = False
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    replace_line = line
                    c = 1
                    for m in matches:
                        re_write = True
                        link = parse_metadata_link(m[1])
                        if link is None:
                            print 'other:', m[1]
                            print path
                        else:
                            replace_link = get_replace_link(link)
                            if replace_link is None:
                                not_founds.append(m[0] + m[1])
                                continue
                            download = m[0] + m[1]
                            replacer = re.compile(download)
                            replace_line = replacer.sub(replace_link, replace_line)
                            c += 1
                            data_links.append(download)

                    lines.append(replace_line)
                else:
                    lines.append(line)
        if re_write:
            with open('data/ftplinktest/replaces_preview/preview' + str(preview_name) + '.html', 'w') as re_file:
                for line in lines:
                    re_file.write(line)
            if rewrite_source:
                with open(path, 'w') as re_file:
                    for line in lines:
                        re_file.write(line)

        return data_links

    preview_count = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = check_ftp_links_in_file(dir_path, ftp_link_matcher, preview_count)
            if links > 0:
                preview_count += 1
            data_paths.extend(links)

    # for n in not_founds:
    #     print n
    print len(set(data_paths))
    print len(set(not_founds))
    return set(data_paths)


def replace_direct_package_links(top_dir='data/ftplinktest', rewrite_source=False):
    ftp_link_matcher = ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/SGID93_Vector/NAD83/MetadataHTML)(.+?)[\"\)]')
    data_paths = []
    not_founds = []

    def get_all_direct_link_matchers(direct_link_json='data/direct_packages.json'):
        direct_links = []
        packages = None
        with open(direct_link_json, 'r') as json_file:
            packages = json.load(json_file)
        for p in packages:
            for zip_name in packages[p]:
                direct_links.append(re.compile(zip_name.values()[0].replace('https://drive.google.com/a/utah.gov/uc?id=', '').replace('&export=download', '')))
        return direct_links

    def get_replace_link(link):
        new_prefix = 'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        new_html_file_path = '/Volumes/ftp/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        if not os.path.exists(new_html_file_path + 'SGID10.' + link + '.xml'):
            print link, 'does not exist as new'
            return None
        else:
            return new_prefix + 'SGID10.' + link + '.xml'

    def check_direct_links_in_file(path, direct_links, preview_name):
        data_links = []
        lines = []
        re_write = False
        with open(path, 'r') as search_file:
            for line in search_file:
                for link_matcher in direct_links:
                    matches = link_matcher.findall(line)
                    if len(matches) > 0:
                        print search_file
                        print line
                        data_links.append(line)

                    # if link in line:
                    #     data_links.append(line)
                # matches = matcher.findall(line)
                # if len(matches) > 0:
                #     data_links.append(line)
                #     replace_line = line
                #     c = 1
                #     for m in matches:
                #         re_write = True
                #         link = parse_metadata_link(m[1])
                #         if link is None:
                #             print 'other:', m[1]
                #             print path
                #         else:
                #             replace_link = get_replace_link(link)
                #             if replace_link is None:
                #                 not_founds.append(m[0] + m[1])
                #                 continue
                #             download = m[0] + m[1]
                #             replacer = re.compile(download)
                #             replace_line = replacer.sub(replace_link, replace_line)
                #             c += 1
                #             data_links.append(download)
                #
                #     lines.append(replace_line)
                # else:
                #     lines.append(line)
        if re_write:
            with open('data/ftplinktest/replaces_preview/preview' + str(preview_name) + '.html', 'w') as re_file:
                for line in lines:
                    re_file.write(line)
            if rewrite_source:
                with open(path, 'w') as re_file:
                    for line in lines:
                        re_file.write(line)

        return data_links

    preview_count = 0
    links = None
    direct_links = get_all_direct_link_matchers()
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = check_direct_links_in_file(dir_path, direct_links, preview_count)
# `            if links > 0:
#                 preview_count += 1
#             data_paths.extend(links)`

    # for n in not_founds:
    #     print n
    print len(links)
    # print len(set(not_founds))
    # return set(data_paths)


def replace_ftp_links(top_dir='data/ftplinktest', rewrite_source=False):
    ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+?)[\"\)]')
    data_paths = []
    ided_features = get_spec_catnames(spec_manager.get_feature_spec_path_list(), True)
    ided_packages = get_spec_catnames(spec_manager.get_package_spec_path_list(), True)

    not_founds = []

    def get_replace_link(link, spec):
        last_7 = link.path[-7:]
        zip_id = None
        if link.ext == '.zip' and last_7 in ['shp.zip', 'gdb.zip']:
            if 'shp' in last_7:
                zip_id = driver.get_download_link(spec['shape_id'])
            elif 'gdb' in last_7:
                zip_id = driver.get_download_link(spec['gdb_id'])
            return '{}'.format(zip_id)
        elif link.ext is None:
            return '{}'.format(driver.get_webview_link(spec['parent_ids'][0]))

    def check_ftp_links_in_file(path, matcher, feature_specs, package_specs, preview_name):
        data_links = []
        lines = []
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    replace_line = line
                    c = 1
                    for m in matches:
                        link = parse_ftp_link(m[1], top_dir)
                        if link is None:
                            print 'other:', m[1]
                            print path
                        elif (not link.packaged and link.get_catname() in feature_specs):
                            replace_link = get_replace_link(link, feature_specs[link.get_catname()])
                            if replace_link is None:
                                print 'other:', link.path
                                continue
                            download = m[0] + m[1]
                            replacer = re.compile(download)
                            replace_line = replacer.sub(replace_link, replace_line)
                            c += 1
                            data_links.append(download)
                        elif (link.packaged and link.get_catname() in package_specs):
                            replace_link = get_replace_link(link, package_specs[link.get_catname()])
                            if replace_link is None:
                                print 'other:', link.path
                                continue
                            download = m[0] + m[1]
                            replacer = re.compile(download)
                            replace_line = replacer.sub(replace_link, replace_line)
                            c += 1
                            data_links.append(download)
                        else:
                            print 'not found:', link.path
                            if link.packaged:
                                not_founds.append(link.name)
                            print path

                    lines.append(replace_line)
                else:
                    lines.append(line)

        with open('data/ftplinktest/replaces_preview/preview' + str(preview_name) + '.html', 'w') as re_file:
            for line in lines:
                re_file.write(line)
        if rewrite_source:
            with open(path, 'w') as re_file:
                for line in lines:
                    re_file.write(line)

        return data_links
    preview_count = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = check_ftp_links_in_file(dir_path, ftp_link_matcher, ided_features, ided_packages, preview_count)
            if links > 0:
                preview_count += 1
            data_paths.extend(links)

    # for n in not_founds:
    #     print n
    return data_paths


def parse_metadata_link(link):
    link_parts = link.split('_')[1:]
    category = link_parts[0].upper()
    name = '_'.join(link_parts[1:]).replace('.html', '')
    return '{}.{}'.format(category, name)

def parse_ftp_link(link, src_dir):
    link_parts = link.split('/')[1:]
    category = link_parts[0].upper()
    name = None
    packaged = None
    src_dir = src_dir
    ext = None
    if link_parts[1].lower() == 'packageddata':
        packaged = True
        name = link_parts[3]
    elif link_parts[1].lower() == 'unpackageddata':
        packaged = False
        name = link_parts[2]
    else:
        return None

    if '.' in link:
        ext = link[link.rfind('.'):]

    uniquer = "{}_{}_{}".format(category, name, packaged)
    if uniquer in FtpLink.unique_links:
        print uniquer, link
        return None

    return FtpLink(category, name, packaged, src_dir, ext, link)


def get_spec_catnames(spec_path_list, only_if_gdbid=False):
    import spec_manager
    specs = [spec_manager.load_feature_json(path) for path in spec_path_list]
    catname_dict = {}
    if only_if_gdbid:
        for s in specs:
            if s['gdb_id'] != '':
                catname_dict["{}_{}".format(s['category'], s['name']).lower()] = s
        # catname_list = ["{}_{}".format(x['category'], x['name']).lower() for x in specs if x['gdb_id'] != '']
    else:
        catname_dict["{}_{}".format(s['category'], s['name']).lower()] = s
        # catname_list = ["{}_{}".format(x['category'], x['name']).lower() for x in specs]
    return catname_dict


def create_new_features(spec_catnames, ftp_catname_dict, workspace):
    import arcpy
    not_found = []
    for catname in ftp_catname_dict:
        if catname not in spec_catnames:
            ftp_link = ftp_catname_dict[catname]
            fc = "{}.{}.{}".format('SGID10', ftp_link.category.upper(), ftp_link.name)
            if arcpy.Exists(os.path.join(workspace, fc)):
                spec_manager.get_feature(fc, create=True)
                print catname
            else:
                not_found.append(ftp_link)
    return not_found


def get_not_found_packages(non_feature_ftp_links, package_names):
    not_package = []
    not_found = non_feature_ftp_links
    for nf in not_found:
        if nf.name.lower() not in package_names:
            not_package.append(nf)

    return not_package


def get_name_folder_id(name, parent_id):
    """Get drive id for a folder with name of category and in parent_id drive folder."""
    category_id = user_drive.get_file_id_by_name_and_directory(name, parent_id)
    if not category_id:
        print 'Creating drive folder: {}'.format(name)
        category_id = user_drive.create_drive_folder(name, [parent_id])

    return category_id


def reassign_feature_parents():
    ided_feature_specs = get_spec_catnames(spec_manager.get_feature_spec_path_list(), True)
    for spec_name in ided_feature_specs:
        print spec_name
        spec = ided_feature_specs[spec_name]
        old_parent_id = spec['parent_ids'][0]
        new_parent_id = get_name_folder_id(spec['name'], old_parent_id)
        user_drive.change_file_parent(spec['gdb_id'], old_parent_id, new_parent_id)
        user_drive.change_file_parent(spec['shape_id'], old_parent_id, new_parent_id)
        spec['parent_ids'] = [new_parent_id]
        spec_manager.save_spec_json(spec)


def get_folder_id(name, parent_id):
    """Get drive id for a folder with name of name and in parent_id drive folder."""
    name_id = user_drive.get_file_id_by_name_and_directory(name, parent_id)
    if not name_id:
        print 'Creating drive folder: {}'.format(name)
        name_id = user_drive.create_drive_folder(name, [parent_id])

    return name_id


def get_hash_size_csv():
    features = spec_manager.get_feature_specs()
    out_csv = 'data/hash_sizes'
    hash_size_records = [['name', 'hash_size', 'cycle']]
    for feature in features:
        if feature['hash_id'] == "":
            continue
        name = feature['sgid_name']
        print name
        size = user_drive.get_size(feature['hash_id'])
        time.sleep(0.5)
        cycle = feature['update_cycle']
        print '\t', size
        hash_size_records.append([name, size, cycle])

    with open(out_csv, 'wb') as out_table:
        table = csv.writer(out_table)
        table.writerows(hash_size_records)


def get_spec_property_csv(properties):
    features = spec_manager.get_feature_specs()
    output_rows = []
    out_csv = 'data/properties.csv'
    count = 0
    for feature in features:
        if count % 50 == 0:
            print count
        count += 1
        out_row = [feature[p] for p in properties]
        if feature['gdb_id'] == "":
            print feature['sgid_name']
            continue
        out_row.append(float(user_drive.get_size(feature['gdb_id'])) / 1048576)
        time.sleep(0.01)
        output_rows.append(out_row)

    print count
    with open(out_csv, 'wb') as out_table:
        table = csv.writer(out_table)
        table.writerow(properties + ['MB'])
        table.writerows(output_rows)


def get_total_data_size():
    features = spec_manager.get_feature_specs()
    sizes =[]
    for feature in features:
        if feature['gdb_id'] == "" or feature['shape_id'] == "":
            continue
        name = feature['sgid_name']
        print name
        size = user_drive.get_size(feature['gdb_id']) * 0.000001
        size += user_drive.get_size(feature['shape_id']) * 0.000001
        time.sleep(0.1)
        print '\t', size
        sizes.append(size)
        if len(sizes) == 10:
            return

    print 'Total feature MBs:', sum(sizes)
    packages = spec_manager.get_package_specs()
    for package in packages:
        if package['gdb_id'] == "" or package['shape_id'] == "":
            continue
        name = package['name']
        print name
        size = user_drive.get_size(package['gdb_id']) * 0.000001
        size += user_drive.get_size(package['shape_id']) * 0.000001
        time.sleep(0.1)
        print '\t', size
        sizes.append(size)

    print 'total specs:', len(features) + len(packages)
    print 'total sizes:', len(sizes)
    print 'Total MBs:', sum(sizes)


def set_cycle_by_date_in_name():
    dated = re.compile(r'\d{4}')
    for feature in spec_manager.get_feature_specs():
        sgid_name = feature['sgid_name']
        matches = dated.findall(sgid_name)
        if len(matches) == 1 and feature['update_cycle'] == 'day':
            print sgid_name, matches
            feature['update_cycle'] = spec_manager.UPDATE_CYCLES.NEVER
            spec_manager.save_spec_json(feature)


def set_cycle_by_csv():
    update_csv = 'data/update_cycle.csv'
    update_cycles = {}

    with open(update_csv, 'rb') as cycles:
        reader = csv.DictReader(cycles)
        for row in reader:
            name = row['SGID name']
            update = row['Update frequency']
            if update == 'on-demand':
                update = 'demand'
            update_cycles[name] = update

    for feature in spec_manager.get_feature_specs():
        sgid_name = feature['sgid_name']
        if sgid_name in update_cycles:
            feature['update_cycle'] = update_cycles[sgid_name]
            spec_manager.save_spec_json(feature)
            # print sgid_name, feature['update_cycle'], update_cycles[sgid_name]
        else:
            print sgid_name, 'not found!!!'

def check_empty_gdb_ids():
    features = spec_manager.get_feature_specs()
    for feature in features:
        if feature['gdb_id'] == "":
            cat_id = user_drive.get_file_id_by_name_and_directory(feature['category'], '0ByStJjVZ7c7mNlZRd2ZYOUdyX2M')
            f_id = user_drive.get_file_id_by_name_and_directory(feature['name'], cat_id)
            time.sleep(0.01)
            if f_id is None:
                print "'{}',".format(feature['sgid_name'])


def check_feature_in_packages(sgid_name_list):
    packages = spec_manager.get_package_specs()
    for package in packages:
        print package['name']
        for f in package['feature_classes']:
            if f in packages:
                print '\t', f


def replace_paths_in_stewardship():
    spreadsheet = '11ASS7LnxgpnD0jN4utzklREgMf1pcvYjcXcIcESHweQ'
    sheet = 'SGID Stewardship Info'
    column = 'k'
    paths = user_sheets.get_column(spreadsheet, sheet, column)

    ided_features = get_spec_catnames(spec_manager.get_feature_spec_path_list(), True)
    ided_packages = get_spec_catnames(spec_manager.get_package_spec_path_list(), True)
    drive_paths = []
    for i, path in enumerate(paths):
        drive_link = replace_ftp_link(path, ided_features, ided_packages)
        if drive_link is not None:
            paths[i] = drive_link
            drive_paths.append(drive_link)
        else:
            paths[i] = None

    print len(drive_paths)
    user_sheets.replace_column(spreadsheet, sheet, column, paths)
    # for i, path in enumerate(paths, start=2):
    #     print i, path


def add_permissions(category, user_email):
    features = spec_manager.get_feature_specs()
    for feature in features:
        if category is None or category.upper() == feature['category'].upper():
            ids = [
                feature['gdb_id'],
                feature['hash_id'],
                feature['shape_id']
            ]
            for file_id in ids:
                print user_drive.add_editor(file_id, user_email), feature['name']
                time.sleep(0.2)


def find_id(drive_id):
    features = spec_manager.get_feature_specs()
    for feature in features:
        try:
            ids = [
                feature['gdb_id'],
                feature['hash_id'],
                feature['shape_id'],
                feature['parent_ids'][0]
            ]
        except IndexError:
            print 'no parents', feature['name']

        if drive_id in ids:
            print feature['name']


def write_new_page(old_html, new_xml_url):
    new_page = """
    <!DOCTYPE html>
    <html>
        <body>

        <h1>This file has moved</h1>
        <a href="{}">New FGDC metadata</a>

        </body>
    </html>
    """.format(new_xml_url)
    with open(old_html, 'w') as old_file:
        old_file.write(new_page)


def get_new_metadata_url(old_metadata_url):
    metadata_link_matcher = re.compile(r'[\"\(]?(ftp://ftp\.agrc\.utah\.gov/SGID93_Vector/NAD83/MetadataHTML)(.+)[\"\)]?')
    matches = metadata_link_matcher.findall(old_metadata_url)
    if len(matches) > 0:
        link = matches[0][1]
        l = parse_metadata_link(link)
        ftp_metadata = 'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        new_xml_path = os.path.join(ftp_metadata, 'SGID10.' + l + '.xml')
        return new_xml_path
    else:
        raise(Exception('url parse error'))

def find_old_metadata():
    # : Find all ftp Metadata links
    all_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov')
    all_ftp_paths = get_all_ftp_links(all_dir)
    ftp_metadata = []
    old_html_prefix = '/Volumes/ftp/SGID93_Vector/NAD83/MetadataHTML'
    for path in all_ftp_paths:
        # print old_html_prefix + path
        ftp_metadata.append(path.strip())

    # with open('data/not_found_meta.csv', 'wb')
    ftp_metadata = set(ftp_metadata)
    print 'Old links on site', len(ftp_metadata)
    for path in ftp_metadata:
        l = parse_metadata_link(path)
        ftp_metadata = '/Volumes/ftp/UtahSGID_Vector/UTM12_NAD83/Metadata/'
        new_xml_path = os.path.join(ftp_metadata, 'SGID10.' + l + '.xml')
        new_xml_name = 'SGID10.' + l + '.xml'
        if not os.path.exists(new_xml_path):
            print '\'SGID10.' + l + "',"
            # print path
            pass
        else:
            #write_new_page(old_html_prefix + path, new_xml_name)
            print new_xml_path
            pass


def replace_old_metadata():
    all_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov')
    replaced_links = replace_metadata_links(all_dir, rewrite_source=True)
    return replaced_links


def get_feature_download_links():
    features = spec_manager.get_feature_specs()
    feature_links = {}
    for feature in features:
        feature_links[feature['sgid_name'].lower()] = {
            'gdb': driver.get_download_link(feature['gdb_id']),
            'shp': driver.get_download_link(feature['shape_id'])
        }
    spec_manager.save_spec_json(feature_links, 'data/feature_downloads.json')


def create_old_package_json(folder_id):
    packages = {}
    name_ids = user_drive.list_files_in_directory(folder_id)
    print len(name_ids)
    for name, id_number in name_ids:
        package_name = name.replace('_gdb.zip', '').replace('_shp.zip', '')
        if package_name not in packages:
            packages[package_name] = []
        packages[package_name].append({name: driver.get_download_link(id_number)})
    with open('data/direct_packages.json', 'wb') as p_file:
        p_file.write(json.dumps(packages, sort_keys=True, indent=4))


if __name__ == '__main__':
    import argparse
    home_dir = os.path.expanduser('~')
    if os.path.exists('data/ftplinktest/replaces_preview'):
        shutil.rmtree('data/ftplinktest/replaces_preview')
    os.makedirs('data/ftplinktest/replaces_preview')
    print 'Temp directory removed'

    parser = argparse.ArgumentParser(description='Update links')

    parser.add_argument('-r', action='store_true', dest='rewrite_source',
                        help='Rewrite file in the source directory')
    parser.add_argument('-c', action='store', dest='feature_category',
                        help='Looks in data/feature_category')
    parser.add_argument('--list_by_subdir', action='store', dest='top_dir',
                        help='Lists ftp links in subdirs')
    args = parser.parse_args()

    if args.feature_category:
        data_dir = '/Users/kwalker/Documents/repos/gis.utah.gov/data/' + args.feature_category

        paths = replace_ftp_links(data_dir, rewrite_source=args.rewrite_source)
        print 'UPDATED'
        for p in paths:
            print p
    if args.top_dir:
        list_ftp_links_by_subfolder('/Users/kwalker/Documents/repos/gis.utah.gov/' + args.top_dir)

    # print 'day', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.DAY))
    # print 'week', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.WEEK))
    # print 'month', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.MONTH))
    # print 'quarter', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.QUARTER))
    # print 'biannual', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.BIANNUAL))
    # print 'annual', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.ANNUAL))
    # print 'never', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.NEVER))

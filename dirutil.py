import os
import hashlib
import re
import json
import shutil
import csv
import time

import spec_manager
import driver
user_drive = driver.AgrcDriver(secrets=driver.OAUTH_CLIENT_SECRET_FILE, use_oauth=True)


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
    # import pdb; pdb.set_trace()
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
                spec_manager.get_feature(fc)
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


def reassign_package_parents():
    """store all zipped packages in folder for backup in preperation for package folder views."""
    package_specs = spec_manager.get_package_specs()
    for spec in package_specs:
        old_parent_id = spec['parent_ids'][0]
        new_parent_id = '0ByStJjVZ7c7mVHp0V2lfVWgxdFU'
        time.sleep(0.1)
        user_drive.change_file_parent(spec['gdb_id'], old_parent_id, new_parent_id)
        user_drive.change_file_parent(spec['shape_id'], old_parent_id, new_parent_id)
        spec['gdb_id'] = ''
        spec['shape_id'] = ''
        spec_manager.save_spec_json(spec)


def get_folder_id(name, parent_id):
    """Get drive id for a folder with name of name and in parent_id drive folder."""
    name_id = user_drive.get_file_id_by_name_and_directory(name, parent_id)
    if not name_id:
        print 'Creating drive folder: {}'.format(name)
        name_id = user_drive.create_drive_folder(name, [parent_id])

    return name_id


def add_features_to_package_folder():
    """store all zipped packages in folder for backup in preperation for package folder views."""
    package_specs = spec_manager.get_package_specs()
    for spec in package_specs:
        if spec['gdb_id'] != '':
            print 'skip', spec['name']
            continue
        package_folder_id = spec['parent_ids'][0]
        package_name = spec['name']
        print package_name
        gdb_folder_id = get_folder_id(package_name + '_gdb', package_folder_id)
        shp_folder_id = get_folder_id(package_name + '_shp', package_folder_id)
        spec['gdb_id'] = gdb_folder_id
        spec['shape_id'] = shp_folder_id
        spec_manager.save_spec_json(spec)

        feature_names = spec['feature_classes']
        for name in feature_names:
            print '\t', name
            feature = spec_manager.get_feature(name)
            time.sleep(0.02)
            user_drive.add_file_parent(feature['gdb_id'], gdb_folder_id)
            time.sleep(0.02)
            user_drive.add_file_parent(feature['shape_id'], shp_folder_id)


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


def set_cycle_by_hash_size():
    hash_sizes = 'data/hash_sizes.csv'
    with open(hash_sizes, 'rb') as info:
        reader = csv.DictReader(info)
        for row in reader:
            if int(row['hash_size']) < 1000000 and row['cycle'] == "":
                print row['name']
                feature = spec_manager.get_feature(row['name'])
                feature['update_cycle'] = spec_manager.UPDATE_CYCLES.DAY
                spec_manager.save_spec_json(feature)


def set_cycle_by_date_in_name():
    dated = re.compile(r'\d{4}')
    for feature in spec_manager.get_feature_specs():
        sgid_name = feature['sgid_name']
        matches = dated.findall(sgid_name)
        if len(matches) == 1 and feature['update_cycle'] == 'day':
            print sgid_name, matches
            feature['update_cycle'] = spec_manager.UPDATE_CYCLES.NEVER
            spec_manager.save_spec_json(feature)



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

    get_total_data_size()

    # print 'day', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.DAY))
    # print 'week', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.WEEK))
    # print 'month', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.MONTH))
    # print 'quarter', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.QUARTER))
    # print 'biannual', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.BIANNUAL))
    # print 'annual', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.ANNUAL))
    # print 'never', len(spec_manager.get_feature_specs(spec_manager.UPDATE_CYCLES.NEVER))


    # for f in [s['sgid_name'] for s in spec_manager.get_feature_specs('') if s['update_cycle'] == '']:
    #     print f
    # packages = set()
    # for f in spec_manager.get_feature_specs():
    #     p = f['parent_ids']
    #     packages.update(p)
    #     if len(p) > 1:
    #         print f['sgid_name']


    # for p in packages:
    #     if not os.path.exists(os.path.join('packages', p + '.json')):
    #         print p

    # get_update_cycles('data/steward_info.csv')
    # feature_update_types = get_feature_update_cycles('data/steward_info.csv')
    # for feature in feature_update_types:
    #     update = feature_update_types[feature]
    #     if update in ['1', 'constant', 'internal']:
    #         print feature
    #         set_spec_update_types(feature, 'day')

    # package_specs_from_gdbs(r'C:\GisWork\temp\aaaPackage', 'INDICES')

    #: Find all ftp links
    # data_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/data')
    # datas = get_all_ftp_links(data_dir)
    # post_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/_posts')
    # posts = get_all_ftp_links(post_dir)
    # ftp_links = []
    #
    # for path in datas:
    #     ftp_links.append(parse_ftp_link(path, 'data'))
    # for p in posts:
    #     ftp_links.append(parse_ftp_link(path, '_posts'))
    # ftp_links = [l for l in ftp_links if l is not None]
    # print 'total links', len(ftp_links)
    #
    # datas.extend(posts)
    # unique_links = set(datas)
    # exts = [p[p.rfind('.'):] for p in unique_links if '.' in p]
    # print 'ext files', len(exts)
    # print set(exts)
    # print 'unique links', len(unique_links)
    # ftp_catnames = {}
    # for fl in ftp_links:
    #     if fl.ext is None or fl.ext == '.zip':
    #         ftp_catnames["{}:{}".format(fl.category, fl.name).lower()] = fl
    #
    # spec_catnames = get_feature_catnames()
    # not_found = [ftp_catnames[n] for n in ftp_catnames if n not in spec_catnames]
    # # not_found = create_new_features(spec_catnames, ftp_catnames, r'Database Connections\Connection to sgid.agrc.utah.gov.sde')
    # package_names = [n.replace('.json', '').lower() for n in spec_manager.get_package_spec_path_list()]
    # not_package = get_not_found_packages(not_found, package_names)
    # print 'non-feature, not found packages', len(not_package)
    #
    # ftp_packaged_data = [f for f in ftp_links if f.packaged]
    # print 'total packagedData paths', len(ftp_packaged_data)
    # not_package_at_all = get_not_found_packages(ftp_packaged_data, package_names)
    # not_package_is_feature = []
    # not_packge_not_feature_paths = [f.path for f in not_package]
    # for p in not_package_at_all:
    #     if p.path[:p.path.rfind('/') + 1] not in not_packge_not_feature_paths:
    #         not_package_is_feature.append(p.path.strip())
    # print 'Not found packages', len(not_package_at_all)
    #
    # problem_paths = [np.path.strip() for np in not_package]
    # problem_paths.extend(not_package_is_feature)
    # problem_paths.sort()
    # print len(set(problem_paths))
    # with open('data/notfound.txt', 'w') as f_out:
    #     for p in problem_paths:
    #         f_out.write(p + ',\n')

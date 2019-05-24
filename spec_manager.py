"""Code to manage spec json files"""
import os
import json
import argparse


PACKAGE_SPEC_FOLDER = 'packages'
FEATURE_SPEC_FOLDER = 'features'
FEATURE_SPEC_TEMPLATE = 'templates/feature_template.json'
PACKAGE_SPEC_TEMPLATE = 'templates/package_template.json'


class UPDATE_CYCLES(object):
    """Update cycle constants."""

    NEVER = 'demand'
    ANNUAL = 'annual'
    BIANNUAL = 'biannual'
    QUARTER = 'quarter'
    MONTH = 'month'
    WEEK = 'week'
    DAY = 'day'


def valitdate_spec(spec):
    if "" in spec['parent_ids']:
        msg = 'Invalid spec: {}'.format(spec)
        raise Exception(msg)


def save_spec_json(spec, json_path=None):
    save_path = json_path
    if save_path is None:
        folder = None
        file_name = None
        if 'sgid_name' in spec:
            folder = FEATURE_SPEC_FOLDER
            file_name = create_feature_spec_name(spec['sgid_name'])
        else:
            folder = PACKAGE_SPEC_FOLDER
            file_name = spec['name'] + '.json'

        save_path = os.path.join(folder,
                                 file_name)

    with open(save_path, 'w') as f_out:
        f_out.write(json.dumps(spec, sort_keys=True, indent=4))


def load_feature_json(json_path):
    with open(json_path, 'r') as json_file:
        feature = json.load(json_file)

    return feature


def delete_spec_json(spec):
    folder = None
    file_name = None
    if 'sgid_name' in spec:
        folder = FEATURE_SPEC_FOLDER
        file_name = create_feature_spec_name(spec['sgid_name'])
    else:
        folder = PACKAGE_SPEC_FOLDER
        file_name = spec['name'] + '.json'

    delete_path = os.path.join(folder,
                               file_name)
    os.remove(delete_path)


def create_feature_spec_name(source_name):
    spec_name = '_'.join(source_name.split('.')[-2:]) + '.json'
    return spec_name


def create_package_spec(name, feature_classes, category):
    json_path = os.path.join(PACKAGE_SPEC_FOLDER, name + '.json')
    if not os.path.exists(os.path.join('packages', name + '.json')):
        empty_spec = PACKAGE_SPEC_TEMPLATE
        package = load_feature_json(empty_spec)
        package['name'] = name
        package['feature_classes'] = feature_classes
        package['category'] = category
        save_spec_json(package)
        return json_path
    else:
        return json_path


def get_package(package_name):
    spec_name = package_name
    if not package_name.endswith('.json'):
        spec_name += '.json'

    package_spec = os.path.join(PACKAGE_SPEC_FOLDER, spec_name)

    package = None
    if not os.path.exists(package_spec):
        msg = 'Package spec does not exist at {}'.format(package_spec)
        raise Exception(msg)
    else:
        package = load_feature_json(package_spec)
    valitdate_spec(package)

    return package


def get_feature(source_name, packages=[], create=False):
    empty_spec = FEATURE_SPEC_TEMPLATE
    spec_name = create_feature_spec_name(source_name)
    feature_spec = os.path.join(FEATURE_SPEC_FOLDER, spec_name)
    feature = None
    if not os.path.exists(feature_spec):
        feature = load_feature_json(empty_spec)
        feature['sgid_name'] = source_name
        feature['name'] = source_name.split('.')[-1]
        feature['category'] = source_name.split('.')[-2]
        feature['packages'].extend(packages)
    else:
        try:
            feature = load_feature_json(feature_spec)
        except ValueError, e:
            print '!bad json!:', source_name
            raise(e)
        for p in packages:
            if p not in feature['packages']:
                feature['packages'].append(p)
    valitdate_spec(feature)
    save_spec_json(feature)
    return feature


def _add_feature_to_package(package_name, feature_source_name):
    """depreciated"""
    package = get_package(package_name)
    if feature_source_name not in package['feature_classes']:
        package['feature_classes'].append(feature_source_name)
    save_spec_json(package)


def _remove_feature_from_package(package_name, feature_source_name):
    """depreciated"""
    package = get_package(package_name)
    if feature_source_name in package['feature_classes']:
        package['feature_classes'].remove(feature_source_name)
        save_spec_json(package)


def _add_package_to_feature(source_name, package_name):
    """depreciated"""
    add_feature_to_package(package_name, source_name)
    feature = get_feature(source_name, [package_name], create=True)
    save_spec_json(feature)


def get_package_spec_path_list():
    packages = []
    for root, subdirs, files in os.walk(PACKAGE_SPEC_FOLDER):
        for filename in files:
            if filename == '.DS_Store':
                continue
            packages.append(os.path.join(PACKAGE_SPEC_FOLDER, filename))
        break
    return packages


def get_feature_spec_path_list():
    features = []
    for root, subdirs, files in os.walk(FEATURE_SPEC_FOLDER):
        for filename in files:
            if filename == '.DS_Store':
                continue
            features.append(os.path.join(FEATURE_SPEC_FOLDER, filename))
        break
    return features


def get_feature_specs(update_cycles=None):
    selected_cycles = update_cycles
    if type(selected_cycles) == str:
        selected_cycles = [update_cycles]

    feature_specs = []
    for f in get_feature_spec_path_list():
        spec = load_feature_json(f)
        if update_cycles is None or len(update_cycles) == 0 or spec['update_cycle'] in selected_cycles:
            feature_specs.append(spec)

    return feature_specs


def get_package_specs():
    package_specs = []
    for p in get_package_spec_path_list():
        spec = load_feature_json(p)
        package_specs.append(spec)

    return package_specs


def add_update():
    for f in get_feature_spec_path_list():
        spec = load_feature_json(f)
        if 'update_cycle' not in spec:
            spec['update_cycle'] = ""
            save_spec_json(spec)


def _list_packages_with_nonexistant_features(workspace, package_list=None):
    """List packages with features that do not exist in the workspace."""
    import arcpy
    bad_features = []
    bad_packages = {}
    packages_to_check = package_list
    if package_list is None:
        packages_to_check = get_package_spec_path_list()

    for p in packages_to_check:
        if not p.endswith('.json'):
            p += '.json'
        packages_spec = get_package(os.path.basename(p))
        fcs = packages_spec['feature_classes']
        if fcs != '' and len(fcs) > 0:
            for f in fcs:
                if f in bad_features or not arcpy.Exists(os.path.join(workspace, f)):
                    if f not in bad_features:
                        print f
                        bad_features.append(f)
                    if p not in bad_packages:
                        bad_packages[p] = []
                    bad_packages[p].append(f)
    print
    for p in bad_packages:
        print p
        for f in bad_packages[p]:
            print ' ', f


def _list_nonexistant_features(workspace):
    """List features that do not exist in the workspace."""
    import arcpy
    bad_features = []
    features_to_check = get_feature_specs()
    for feature_spec in features_to_check:
        fc = feature_spec['sgid_name']
        if fc in bad_features or not arcpy.Exists(os.path.join(workspace, fc)):
            if fc in bad_features:
                print 'TWICE!!!!', fc
            bad_features.append(fc)

    for f in bad_features:
        print f


def _clear_driveids(path, spec):
    if 'hash_id' in spec:
        spec['hash_id'] = ''

    spec['gdb_id'] = ''
    spec['shape_id'] = ''
    spec['parent_ids'] = []
    save_spec_json(spec)


def clear_all_drive_ids():
    for path in get_feature_spec_path_list():
        spec = load_feature_json(path)
        _clear_driveids(path, spec)

    for path in get_package_spec_path_list():
        spec = load_feature_json(path)
        _clear_driveids(path, spec)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update metadata from docs')#, parents=[tools.argparser])

    parser.add_argument('-c', action='store_true', dest='create',
                        help='Create a feature spec if it does not exist')
    parser.add_argument('source_name', action='store',
                        help='Source name for the feature')

    args = parser.parse_args()
    if args.create:
        get_feature(args.source_name, create=True)


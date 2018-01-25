from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseDownload, MediaIoBaseUpload
import httplib2
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.service_account import ServiceAccountCredentials

import io
import os
from time import sleep
from random import uniform

SCOPES = 'https://www.googleapis.com/auth/drive'
SERVICE_ACCOUNT_SECRET_FILE = 'service_secret.json'
# oauth2
OAUTH_CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'SGID on Drive'

flags = None


class APIS(object):
    drive = ('drive', 'v3')
    sheets = ('sheets', 'v4')



class flags_shim(object):

    def __init__(self):
        self.auth_host_name = 'localhost'
        self.noauth_local_webserver = False
        self.auth_host_port = [8080, 8090]
        self.logging_level = 'ERROR'


class ApiService(object):
    def __init__(self, apis, secrets=SERVICE_ACCOUNT_SECRET_FILE, scopes=SCOPES, use_oauth=False):
        self.services = []
        for api_name, api_version in apis:
            if use_oauth:
                self.services.append(self.setup_oauth_service(secrets, scopes, api_name, api_version))
            else:
                self.services.append(self.setup_account_service(secrets, scopes, api_name, api_version))

    def get_oauth_credentials(self, secrets, scopes, application_name=APPLICATION_NAME, flags=flags):
        """
        Get valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.
        Returns:
            Credentials, the obtained credential.

        """
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'sgid-drive-loader.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(secrets, scopes)
            flow.user_agent = application_name
            if flags is None:
                flags = flags_shim()
            credentials = tools.run_flow(flow, store, flags)
            print('Storing credentials to ' + credential_path)

        return credentials

    def get_credentials(self, secrets, scopes):
        """Get service account credentials from json key file."""
        credentials = ServiceAccountCredentials.from_json_keyfile_name(secrets, scopes)

        return credentials

    def setup_oauth_service(self, secrets, scopes, api_name, api_version):
        credentials = self.get_oauth_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build(api_name, api_version, http=http)

        return service

    def setup_account_service(self, secrets, scopes, api_name, api_version):
        # get auth
        credentials = self.get_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build(api_name, api_version, http=http)

        return service


class AgrcDriver(object):
    FULL_SCOPE = 'https://www.googleapis.com/auth/drive'

    def __init__(self, api_service):
        self.service = api_service

    def set_property(self, file_id, property_dict):
        if not self.service:
            self.service = self.setup_account_service()
        file_name = self.service.files().update(fileId=file_id,
                                                fields='name',
                                                body={'properties': property_dict}).execute()
        return file_name

    def get_property(self, file_id, property_name):
        if not self.service:
            self.service = self.setup_account_service()
        file_property = self.service.files().get(fileId=file_id,
                                                 fields='properties({})'.format(property_name)).execute()
        return file_property['properties'][property_name]

    def download_file(self, file_id, output):
        import shutil
        request = self.service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        response = False
        backoff = 1
        while response is False:
            try:
                status, response = downloader.next_chunk()
                # if status:
                #     print "Download %d%%." % int(status.progress() * 100)
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Download Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Download Failed: {}'.format(e))
                    print 'Retrying download in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Download Failed \n{}".format(e)
                    raise Exception(msg)

        fh.seek(0)
        with open(output, 'wb') as out_zip:
            shutil.copyfileobj(fh, out_zip, length=131072)

        return True

    def keep_revision(self, file_id, revision_id='head'):
        file_metadata = {'keepForever': True}
        request = self.service.revisions().update(fileId=file_id,
                                                  revisionId=revision_id,
                                                  body=file_metadata,
                                                  fields='id')

        response = None
        backoff = 1
        while response is None:
            try:
                response = request.execute()
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    raise Exception('Update Failed')

        return response.get('id')

    def update_file(self, file_id, local_file, mime_type):
        media_body = MediaFileUpload(local_file,
                                     mimetype=mime_type,
                                     resumable=True)

        request = self.service.files().update(fileId=file_id,
                                              media_body=media_body)

        response = None
        backoff = 1
        while response is None:
            try:
                status, response = request.next_chunk()
            except errors.HttpError, e:
                if e.resp.status in [404]:  # TODO restart on 410 gone
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Upload Failed \n{}".format(e)
                    raise Exception(msg)

        return response.get('id')


    def create_drive_file_from_io(self, name, parent_ids, io_bytes, mime_type, description=None, propertyDict=None):
        file_metadata = {'name': name,
                         'description': description,
                         'mimeType': mime_type,
                         'parents': parent_ids}

        media_body = MediaIoBaseUpload(io_bytes,
                                       chunksize=-1,
                                       mimetype=mime_type,
                                       resumable=True)
        request = self.service.files().create(body=file_metadata,
                                              media_body=media_body,
                                              fields="id")

        response = None
        backoff = 1
        while response is None:
            try:
                status, response = request.next_chunk()
                # if status:
                #     print('{} percent {}'.format(name, int(status.progress() * 100)))
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again or error.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = 'Upload Failed\n{}'.format(e)
                    raise Exception(msg)
        if propertyDict:
            self.set_property(response.get('id'), propertyDict)

        return response.get('id')

    def create_drive_file(self, name, parent_ids, local_file, mime_type, propertyDict=None):
        file_metadata = {'name': name,
                         'mimeType': mime_type,
                         'parents': parent_ids}

        media_body = MediaFileUpload(local_file,
                                     mimetype=mime_type,
                                     resumable=True)
        request = self.service.files().create(body=file_metadata,
                                              media_body=media_body,
                                              fields="id")

        response = None
        backoff = 1
        while response is None:
            try:
                status, response = request.next_chunk()
                # if status:
                #     print('{} percent {}'.format(name, int(status.progress() * 100)))
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again or error.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    raise Exception('Upload Failed')
        if propertyDict:
            self.set_property(response.get('id'), propertyDict)

        return response.get('id')

    def get_file_id_by_name_and_directory(self, name, parent_id):
        response = self.service.files().list(q="name='{}' and '{}' in parents  and explicitlyTrashed=false".format(name,
                                                                                                              parent_id),
                                             spaces='drive',
                                             fields='files(id)').execute()
        files = response.get('files', [])
        if len(files) > 0:
            return files[0].get('id')
        else:
            return None

    def list_files_in_directory(self, parent_id):
        files = []
        page_token = None
        while True:
            response = self.service.files().list(q="'{}' in parents  and explicitlyTrashed=false".format(parent_id),
                                                 spaces='drive',
                                                 fields='nextPageToken, files(id, name)',
                                                 pageToken=page_token).execute()
            for file in response.get('files', []):
                # Process change
                files.append((file.get('name'), file.get('id')))

            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        return files

    def get_size(self, file_id):
        file_size = self.service.files().get(fileId=file_id,
                                             fields='size').execute()
        return int(file_size.get('size'))

    def create_drive_folder(self, name, parent_ids):
        # existing_file_id = get_file_id_by_name_and_directory(name, parent_ids[0], service)
        # if existing_file_id:
        #     print 'Existing file'
        #     return existing_file_id
            # raise Exception('Drive folder {} already exists at: {}'.format(name, existing_file_id))

        file_metadata = {'name': name,
                         'mimeType': 'application/vnd.google-apps.folder',
                         'parents': parent_ids}

        response = self.service.files().create(body=file_metadata,
                                               fields="id").execute()

        return response.get('id')

    def get_parents(self, file_id):
        request = self.service.files().update(fileId=file_id,
                                              fields='id, parents')

        response = None
        backoff = 1
        while response is None:
            try:
                response = request.execute()
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Upload Failed \n{}".format(e)
                    raise Exception(msg)

        return response.get('parents')

    def change_file_parent(self, file_id, old_parent_id, new_parent_id):
        request = self.service.files().update(fileId=file_id,
                                              addParents=new_parent_id,
                                              removeParents=old_parent_id,
                                              fields='id')

        response = None
        backoff = 1
        while response is None:
            try:
                response = request.execute()
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Upload Failed \n{}".format(e)
                    raise Exception(msg)

        return response.get('id')

    def add_file_parent(self, file_id, new_parent_id):
        request = self.service.files().update(fileId=file_id,
                                              addParents=new_parent_id,
                                              fields='id')

        response = None
        backoff = 1
        while response is None:
            try:
                response = request.execute()
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Upload Failed \n{}".format(e)
                    raise Exception(msg)

        return response.get('id')

    def remove_file_parent(self, file_id, parent_id):
        request = self.service.files().update(fileId=file_id,
                                              removeParents=parent_id,
                                              fields='id')

        response = None
        backoff = 1
        while response is None:
            try:
                response = request.execute()
            except errors.HttpError, e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    raise Exception('Upload Failed 404')
                elif e.resp.status in [500, 502, 503, 504]:
                    if backoff > 8:
                        raise Exception('Upload Failed: {}'.format(e))
                    print 'Retrying upload in: {} seconds'.format(backoff)
                    sleep(backoff + uniform(.001, .999))
                    backoff += backoff
                else:
                    msg = "Upload Failed \n{}".format(e)
                    raise Exception(msg)

        return response.get('id')

    def create_owner(self, file_id, email):
        """Transfer ownership of a file."""
        domain_permission = {
            'type': 'user',
            'role': 'owner',
            'emailAddress': email
        }

        req = self.service.permissions().create(
            fileId=file_id,
            body=domain_permission,
            transferOwnership=True,
            fields="id"
        )

        req.execute()

    def add_editor(self, file_id, email):
        domain_permission = {
            'type': 'user',
            'role': 'writer',
            'emailAddress': email
        }

        req = self.service.permissions().create(
            fileId=file_id,
            sendNotificationEmail=False,
            body=domain_permission,
            fields="id"
        )

        return req.execute()

    def delete_file(self, file_id):
        try:
            self.service.files().delete(fileId=file_id).execute()
            return True
        except errors.HttpError as e:
            if e.resp.status in [404]:
                return None
            else:
                raise (e)


def get_download_link(file_id):
    url_formatter = 'https://drive.google.com/a/utah.gov/uc?id={}&export=download'
    return url_formatter.format(file_id)


def get_webview_link(file_id):
    url_formatter = 'https://drive.google.com/drive/folders/{}'
    return url_formatter.format(file_id)


class AgrcSheets(object):
    FULL_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'

    def __init__(self, api_service):
        self.service = api_service

    def append_row(self, spreadsheet_id, sheet_name, row_values):
        # The ID of the spreadsheet to update.
        spreadsheet_id = spreadsheet_id

        # The A1 notation of a range to search for a logical table of data.
        # Values will be appended after the last row of the table.
        range_ = sheet_name

        # How the input data should be interpreted.
        value_input_option = 'USER_ENTERED'

        # How the input data should be inserted.
        insert_data_option = 'INSERT_ROWS'

        value_range_body = {
            'values': row_values
        }

        request = self.service.spreadsheets().values().append(spreadsheetId=spreadsheet_id,
                                                              range=range_,
                                                              valueInputOption=value_input_option,
                                                              insertDataOption=insert_data_option,
                                                              fields='spreadsheetId,updates(updatedRange)',
                                                              body=value_range_body)
        response = request.execute()

    def get_range(self, spreadsheet_id, sheet_name, a1_range):
        # The ID of the spreadsheet to update.
        spreadsheet_id = spreadsheet_id

        # The A1 notation of a range to search
        range_ = '{0}!{1}'.format(sheet_name, a1_range)

        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                           range=range_,
                                                           majorDimension='ROWS')
        return request.execute()['values']

    def get_column(self, spreadsheet_id, sheet_name, column_letter):
        # The ID of the spreadsheet to update.
        spreadsheet_id = spreadsheet_id

        # The A1 notation of a range to search for a logical table of data.
        range_ = '{0}!{1}2:{1}'.format(sheet_name, column_letter)

        request = self.service.spreadsheets().values().get(spreadsheetId=spreadsheet_id,
                                                           range=range_,
                                                           majorDimension='COLUMNS')
        return request.execute()['values'][0]

    def replace_column(self, spreadsheet_id, sheet_name, column_letter, values):
        # The ID of the spreadsheet to update.
        spreadsheet_id = spreadsheet_id

        # The A1 notation of a range to search for a logical table of data.
        # Values will be appended after the last row of the table.
        range_ = '{0}!{1}2:{1}'.format(sheet_name, column_letter)

        # How the input data should be interpreted.
        value_input_option = 'USER_ENTERED'

        value_range_body = {
            'majorDimension': 'COLUMNS',
            'values': [values]
        }

        request = self.service.spreadsheets().values().update(spreadsheetId=spreadsheet_id,
                                                              range=range_,
                                                              valueInputOption=value_input_option,
                                                              body=value_range_body)
        response = request.execute()
        print response


if __name__ == '__main__':
    pass

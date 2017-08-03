from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
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
APPLICATION_NAME = 'Drive API Python Quickstart'

class AgrcDriver(object):
    flags = None

    def __init__(self, secrets=SERVICE_ACCOUNT_SECRET_FILE, scopes=SCOPES, use_oauth=False):
        self.service = None
        if use_oauth:
            self.service = self.setup_oauth_service(secrets, scopes)
        else:
            self.service = self.setup_drive_service(secrets, scopes)

    def get_oauth_credentials(self, secrets, scopes, application_name=APPLICATION_NAME):
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
                                       'drive-python-quickstart.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(secrets, scopes)
            flow.user_agent = application_name
            # if flags:
            #     credentials = tools.run_flow(flow, store, flags)
            credentials = tools.run_flow(flow, store, AgrcDriver.flags)
            print('Storing credentials to ' + credential_path)

        return credentials

    def setup_oauth_service(self, secrets, scopes):
        credentials = self.get_oauth_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        service = discovery.build('drive', 'v3', http=http)

        return service

    def get_credentials(self, secrets, scopes):
        """Get service account credentials from json key file."""
        credentials = ServiceAccountCredentials.from_json_keyfile_name(secrets, scopes)

        return credentials

    def setup_drive_service(self, secrets, scopes):
        # get auth
        credentials = self.get_credentials(secrets, scopes)
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('drive', 'v3', http=http)

        return self.service

    def set_property(self, file_id, property_dict):
        if not self.service:
            self.service = self.setup_drive_service()
        file_name = self.service.files().update(fileId=file_id,
                                                fields='name',
                                                body={'properties': property_dict}).execute()
        return file_name

    def get_property(self, file_id, property_name):
        if not self.service:
            self.service = self.setup_drive_service()
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
                    raise Exception('download Failed')

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
                    raise Exception('Upload Failed')

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
        # keep_version(response.get('id'), drive_service)

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
        # keep_version(response.get('id'), drive_service)

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
        # keep_version(response.get('id'), drive_service)

        return response.get('id')

    def create_owner(self, file_id='0B3yp_Bjfi5sXVDZFWWc0b2dGVkU', email='kwalker@utah.gov'):
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

    def delete_file(self, file_id='0B3yp_Bjfi5sXVDZFWWc0b2dGVkU'):
        print self.service.files().delete(fileId=file_id).execute()


class SgidDriver(AgrcDriver):

    def __init__(self, secrets=SERVICE_ACCOUNT_SECRET_FILE, scopes=SCOPES):
        """Ctor."""
        super(SgidDriver, self).__init__(secrets, scopes)


def get_download_link(file_id):
    url_formatter = 'https://drive.google.com/a/utah.gov/uc?id={}&export=download'
    return url_formatter.format(file_id)


def get_webview_link(file_id):
    url_formatter = 'https://drive.google.com/drive/folders/{}'
    return url_formatter.format(file_id)


if __name__ == '__main__':
    drive = AgrcDriver()
    drive.delete_file()

Application to update Google Drive files with SGID data.

#### Install
- `git clone` repository to your machine
- Create or obtain secret files and add them to local repository
  - Credentials are created in a GCP project at console.cloud.google.com
  - Use a service account for server based file update
    - Drive files need to be shared with the service account user
  - Use application client token for user based file creation and update
    - allows the updates to run on behalf of a user
    - User credentials are needed to create new files so the owner in the utah.gov domain
- Use shell of your choice and navigate to local repository directory
- Run `python zip_loader.py -h` for a list of options
  - Most common usage is: ` python zip_loader.py "path to workspace containing features" --feature "your.full.featurename"`

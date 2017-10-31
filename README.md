Application to update Google Drive files with SGID data.

#### Install
- `git clone` repository to your machine
- Create or obtain secret files and add them to local repository
  - Use a service account for server based file update
  - Use application client token for user based file creation and update
- Use shell of your choice and navigate to local repository directory
- Run `python zip_loader.py -h` for a list of options
  - Most common usage is: ` python zip_loader.py "path to workspace containing features" --feature "your.full.featurename"`

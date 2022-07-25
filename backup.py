from locale import setlocale, LC_ALL
from os import makedirs, popen
from os.path import join as path_join, exists
from sys import exit, stderr
from traceback import format_exc
from argparse import ArgumentParser
from gzip import open as gzip_open

from dropbox.files import WriteMode
from dropbox import DropboxOAuth2FlowNoRedirect, Dropbox

setlocale(LC_ALL,"")
parser = ArgumentParser()
parser.add_argument("-v", help="Verbose", action="store_true", dest="verbose")
parser.add_argument('-H', help="Database host", action="store", dest="db_host", default="localhost")
parser.add_argument('-U', help="Database user", action="store", dest="db_user")
parser.add_argument('-W', help="Database password", action="store", dest="db_password")
parser.add_argument('-p', help="Database port", action="store", dest="db_port", default="5432")
parser.add_argument('-d', help="Database name", action="store", dest="db_name")
parser.add_argument('-o', help="Output folder", action="store", dest="output_folder", default=".")
parser.add_argument('-c', help="Compress with zip", action="store_true", dest="compress")
parser.add_argument('-dk', help="Dropbox App Key", action="store", dest="dropbox_key")
parser.add_argument('-ds', help="Dropbox App Secret", action="store", dest="dropbox_secret")
parser.add_argument('-rf', help="Remove non-compressed dump file", action="store_true", dest="remove_non_compressed")
parser.add_argument('-gt', help="Starts the process of refresh token generation. Execute it before running backup process", action="store_true", dest="get_refresh_token")
args = parser.parse_args()

account_id = None
user_id = None
access_token = None
refresh_token = None
app_key = None
app_secret = None
expires_at = None

REFRESH_TOKEN_FILENAME = "refresh.token"
APP_KEY_FILENAME = "app.key"
APP_SECRET_FILENAME = "app.secret"


if not exists(args.output_folder):
    if args.verbose:
        print(" ".join(("Folder", args.output_folder, "doesn't exist, will be created")))
    try:
        makedirs(args.output_folder)
        if args.verbose:
            print(" ".join(("Folder", args.output_folder, "created")))
    except:
        print(format_exc(), file=stderr)
        exit(1)

if args.get_refresh_token:
    dropbox_key = args.dropbox_key
    dropbox_secret = args.dropbox_secret
    if dropbox_key is None:
        print("Dropbox key cannot be missing.", file=stderr)
        exit(1)
    if dropbox_secret is None:
        print("Dropbox secret cannot be missing.", file=stderr)
        exit(1)
    auth_flow = DropboxOAuth2FlowNoRedirect(dropbox_key, dropbox_secret, token_access_type="offline")
    authorize_url = auth_flow.start()
    print("Prior to obtain the refresh token, is necessary to input the authorization code. Follow these steps:")
    print("1. Go to: " + authorize_url)
    print("2. Click \"Allow\" (you might have to log in first).")
    print("3. Copy the authorization code.")
    auth_code = input("Enter the authorization code here: ").strip()
    try:
        oauth_result = auth_flow.finish(auth_code)
        refresh_token = oauth_result.refresh_token
        with open(REFRESH_TOKEN_FILENAME, "w") as write_file:
            write_file.write(refresh_token)
            print("File refresh.token generated")
        with open(APP_KEY_FILENAME, "w") as write_file:
            write_file.write(dropbox_key)
            print("File app.key generated")
        with open(APP_SECRET_FILENAME, "w") as write_file:
            write_file.write(dropbox_secret)
            print("File app.secret generated")
    except Exception as e:
        print('Error: %s' % (e,), file=stderr)
        exit(1)
else:
    compress = "-Fc" if args.compress else ""
    output_file  = path_join(args.output_folder, args.db_name+".sql")
    command = f"export PGPASSWORD='{args.db_password}'; pg_dump -h {args.db_host} {compress} -U {args.db_user} -p {args.db_port} {args.db_name} > {output_file}"
    if args.verbose:
        print(" ".join(("Creating backup of database", args.db_name, "in", output_file)))
    try:
        result_command = popen(command).read()
        if args.verbose:
            print(result_command)
            print(" ".join(("Created backup of database", args.db_name, "in", output_file)))
    except:
        print(format_exc(), file=stderr)
        exit(1)
    if compress:
        output_file_compressed = output_file+".gz"
        if args.verbose:
            print("Compressing backup")
        try:
            with open(output_file, 'rb') as f_in, gzip_open(output_file_compressed, 'wb') as f_out:
                # TODO allow compress and replace
                f_out.writelines(f_in)
                if args.verbose:
                    print("Backup compressed")
        except:
            print(format_exc(), file=stderr)
            exit(1)
        output_file = output_file_compressed

    if not exists(output_file):
        print(" ".join(("File ", output_file, "doesn't exist")), file=stderr)
    else:
        if not exists(REFRESH_TOKEN_FILENAME):
            print("Refresh token file missing, regenare it running this program with the -gt option and the Dropbox key & secret pair", file=stderr)
            exit(1)
        if not exists(APP_KEY_FILENAME):
            print("App key file missing, regenare it running this program with the -gt option and the Dropbox key & secret pair", file=stderr)
            exit(1)
        if not exists(APP_SECRET_FILENAME):
            print("App secret file missing, regenare it running this program with the -gt option and the Dropbox key & secret pair", file=stderr)
            exit(1)
        else:
            with open(APP_SECRET_FILENAME, "r") as read_file_app_secret, \
                 open(APP_KEY_FILENAME, "r") as read_file_app_key, \
                 open(REFRESH_TOKEN_FILENAME, "r") as read_file_refresh_token:
                app_secret = read_file_app_secret.read()
                app_key = read_file_app_key.read()
                refresh_token = read_file_refresh_token.read()
                with Dropbox(app_key=app_key,app_secret=app_secret, oauth2_refresh_token=refresh_token) as dbx:
                    with open(output_file, 'rb') as f:
                        output_file_name = output_file.split("/")[-1] # TODO allow to choose other path
                        dropbox_path = "".join(("/", output_file_name))
                        if args.verbose:
                            print(" ".join(("Uploading file", output_file_name,"to Dropbox, in path:", dropbox_path)))
                        try:
                            dbx.files_upload(f.read(), dropbox_path, mode=WriteMode('overwrite'))
                            if args.verbose:
                                print(" ".join(("File", output_file_name,"uploaded to Dropbox")))
                        except:
                            print(format_exc(), file=stderr)
                            exit(1)

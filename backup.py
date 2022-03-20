from locale import setlocale, LC_ALL
from os import makedirs, popen
from os.path import join as path_join, exists
from sys import exit
from traceback import format_exc
import dropbox
from dropbox.files import WriteMode
import argparse

setlocale(LC_ALL,"")

parser = argparse.ArgumentParser()
parser.add_argument("-v", help="Verbose", action="store_true", dest="verbose")
parser.add_argument('-H', help="Database host", action="store", dest="db_host", default="localhost")
parser.add_argument('-U', help="Database user", action="store", dest="db_user")
parser.add_argument('-W', help="Database password", action="store", dest="db_password")
parser.add_argument('-p', help="Database port", action="store", dest="db_port", default="5432")
parser.add_argument('-d', help="Database name", action="store", dest="db_name")
parser.add_argument('-o', help="Output folder", action="store", dest="output_folder", default=".")
parser.add_argument('-c', help="Compress with zip", action="store_true", dest="compress")
parser.add_argument('-t', help="Dropbox token", action="store", dest="dropbox_token")
# TODO permitir borrar el archivo al finalizar

args = parser.parse_args()

if not exists(args.output_folder):
    if args.verbose:
        print(" ".join(("El directorio", args.output_folder, "no existe, se creará")))
    try:
        makedirs(args.output_folder)
        if args.verbose:
            print(" ".join(("Directorio", args.output_folder, "creado")))
    except:
        print(format_exc())
        exit()

compress = "-Fc" if args.compress else ""
output_file  = path_join(args.output_folder, args.db_name+".sql")
command = f"export PGPASSWORD='{args.db_password}'; pg_dump -h {args.db_host} {compress} -U {args.db_user} -p {args.db_port} {args.db_name} > {output_file}"

if args.verbose:
    print(" ".join(("Creación de backup de", args.db_name, "en", output_file)))

try:
    result_command = popen(command).read()
    if args.verbose:
        print(result_command)
        print(" ".join(("Creado backup de", args.db_name, "en", output_file)))
except:
    print(format_exc())
    exit()

if compress:
    output_file_compressed = output_file+".gz"
    if args.verbose:
        print("Comprimiendo backup")
    try:
        gzip_command = " ".join(("gzip -c", output_file, ">", output_file_compressed))
        result_command = popen(gzip_command).read()
        if args.verbose:
            print(result_command)
            print("Backup comprimido")
    except:
        print(format_exc())
        exit()
    output_file = output_file_compressed

if not exists(output_file):
    print(" ".join(("El archivo ", output_file, " no existe")))
else:
    output_file = output_file.split("/")[-1]
    # TODO permitir elegir otra ruta
    dropbox_path = "".join(("/", output_file))
    with dropbox.Dropbox(args.dropbox_token, timeout=None) as dbx:
        try:
            dbx.users_get_current_account()
        except:
            print(format_exc())
        else:
            with open(output_file, 'rb') as f:
                if args.verbose:
                    print(" ".join(("Subiendo archivo", output_file,"a Dropbox, en ruta:", dropbox_path)))
                try:
                    dbx.files_upload(f.read(), dropbox_path, mode=WriteMode('overwrite'))
                    if args.verbose:
                        print(" ".join(("Archivo", output_file,"subido a Dropbox")))
                except:
                    print(format_exc())

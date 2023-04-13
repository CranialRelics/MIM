import os
import sys
import shutil
import traceback
from difflib import SequenceMatcher as sm
import tempfile
from ffmpy3 import FFmpeg
import multiprocessing
import argparse

# Bit of code for helping debugging by disabling parallel operations if a debugger is present
PARALLEL = True
gettrace = getattr(sys, 'gettrace', None)
if gettrace is None:
    pass
elif gettrace():  # This is true if we have a debugger attached
    PARALLEL = False

EXTENSIONS = (".flac", ".mp3", ".m4a", ".opus", "m4b")
single_extensions = ("flac", "mp3", "m4a", "m4b")


def copy_recursive(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def similar(a, b):
    # Get a ratio of how similar two strings are
    return sm(None, a, b).ratio()


def build_full_path_from_list(root, files):
    out_list = []
    for f in files:
        out_list.append(os.path.join(root, f))
    return out_list


def build_ffmpeg_file_list(folder, temp_file):
    # This subroutine creates a file that contains all of the files to be converted and concatenated

    if type(folder) is list:
        temp_list = folder
    else:
        for f in build_full_path_from_list(folder, os.listdir(folder)):
            os.rename(f, f.replace(' ', '_').replace("'", ""))
        temp_list = build_full_path_from_list(folder, os.listdir(folder))

    # Check file type
    to_remove = []
    for idx, f in enumerate(temp_list):
        if not f.lower().endswith(EXTENSIONS):
            to_remove.append(idx)
    for idx in sorted(to_remove, reverse=True):
        del temp_list[idx]
    # Try to put everything in order
    temp_list = sorted(temp_list)
    # Write out to file
    for f in temp_list:
        temp_file.write("file '" + f + "'\n")
    temp_file.flush()
    # Nothing to return, we're using a tempfile passed in


def process_single_file(f, return_list):
    out_file = f.split(".")[:-1]
    out_file.append("opus")
    out_file = ".".join(out_file)
    converter = FFmpeg(
        inputs={f: None},
        outputs={out_file: '-strict -2 -ac 1 -c:a opus -b:a 55k -threads 4'}
    )

    converter.run()
    return_list.append(out_file)


def process_folder(folder):
    # For a folder you have to build a text file that has a list of all the files you want to create to provide to ffmpeg
    ffmpeg_file_list = tempfile.NamedTemporaryFile(mode='w+t')
    output_file = folder + ".opus"

    build_ffmpeg_file_list(folder, ffmpeg_file_list)


    converter = FFmpeg(
        inputs={ffmpeg_file_list.name: '-f concat -safe 0'},
        outputs={output_file: '-strict -2 -ac 1 -c:a opus -b:a 55k -threads 4'}
    )

    converter.run()

    ffmpeg_file_list.close()
    return output_file


def process_book_with_sub_folders(root):
    ''' 
    For dealing with subfolders create an opus of the individual files and then we'll concatinate the opus
    This typically happens with CDs of mp3s, so there will be a single opus for each cd that we'll then concatenate 
    together to build the whole book
    '''
    dirs = build_full_path_from_list(root, os.listdir(root))
    source_dir = []
    # Remove everything that isn't a directory (doesn't support mixed setup)
    for dir in dirs:
        if os.path.isdir(dir):
            source_dir.append(dir)
    ffmpeg_file_list = tempfile.NamedTemporaryFile(mode='w+t')
    output_file = root + ".opus"
    files = []
    for dir in source_dir:
        files.append(process_folder(dir))
    build_ffmpeg_file_list(files, ffmpeg_file_list)

    converter = FFmpeg(
        inputs={ffmpeg_file_list.name: '-f concat -safe 0'},
        outputs={output_file: '-c copy'}
    )
    converter.run()

    ffmpeg_file_list.close()

    return output_file


def process_dir(working_dir, return_list=0):
    print("Processing ", working_dir)
    output_files = []
    # Check folder composition (Does it have folders)
    for root, dirs, files in os.walk(working_dir):
        # Process Directories
        if dirs:
            root_name = root.split("/")[-1]
            multiple_books = True

            # Clean up names
            for dir in build_full_path_from_list(root, dirs):
                os.rename(dir, dir.replace(' ', '_').replace("'", ""))
            dirs = os.listdir(root)

            # Check similarity to see if we have CDs or something
            for dir in dirs:
                if similar(root_name, dir) > 0.4 or "CD" in dir:
                    # These are sub folders
                    multiple_books = False
                else:
                    print(dir, "   ", similar(root_name, dir))
            if not multiple_books:
                out_file = process_book_with_sub_folders(root)
                output_files.append(out_file)

        # Process folders with only files
        elif files:
            out_file = process_folder(root)
            output_files.append(out_file)
        # Return values
        if return_list == 0:
            return output_files
        else:
            return_list += output_files


def parse_args():
    parser = argparse.ArgumentParser(description='Script for automating the conversion of audiobooks to opus.')
    parser.add_argument("--workdir", dest="workdir", type=str, default='', help='Specifies the working directory for '
                                                                                'this script. Not specifying this '
                                                                                'parameter assumes the current working '
                                                                                'directory.')
    args = parser.parse_args()

    if args.workdir != '' and not os.path.exists(args.workdir):
        print("Cannot find working directory. This directory must exist and the desired files to convert should be there.")
        sys.exit()
    return args

def main():
    args = parse_args()
    if args.workdir != '':
        work_folder = args.workdir
    else:
        work_folder = os.getcwd()
    working_dir_listing = os.listdir(work_folder)
    failed_to_process = []
    output_files = []
    jobs = []

    # Setup parallelism
    if __name__ == "__main__" and PARALLEL:
        manager = multiprocessing.Manager()
        return_list = manager.list()

    # Build full path of files we are processing
    for idx, dir in enumerate(working_dir_listing):
        working_dir_listing[idx] = os.path.join(work_folder, dir)
    print(working_dir_listing)

    try:
        try:
            output_dir = os.path.join(work_folder, "done")
            os.mkdir(output_dir)
        except:
            pass
        # This handles some issues with ffmpeg, it doesn't like spaces or apostrophes
        for dir in working_dir_listing:
            os.rename(dir, dir.replace(' ', '_').replace("'", ""))

            if os.path.isdir(dir):
                if PARALLEL:
                    p = multiprocessing.Process(target=process_dir, args=(dir, return_list))
                    jobs.append(p)
                    p.start()
                else:
                    output_files += process_dir(dir)
            #Process single files that are left
            else:
                extension = dir.lower().split(".")[-1]
                if os.path.isfile(dir) and extension in single_extensions:
                    if PARALLEL:
                        p = multiprocessing.Process(target=process_single_file, args=(dir, return_list))
                        jobs.append(p)
                        p.start()
                    else:
                        out_file = process_single_file(dir)
        if PARALLEL:
            # Finalize parallel jobs
            for proc in jobs:
                proc.join()
            output_files += return_list

        # Move files to done dir
        for of in output_files:
            try:
                if os.path.exists(of):
                    shutil.move(of, output_dir)
            except Exception:
                print(traceback.format_exc())
                failed_to_process.append(dir)

    except Exception:
        print(traceback.format_exc())
    finally:
        # Cleanup
        os.removedirs(temp_dir)
        # print out if we've had any issues
        print("Failed to process: ", failed_to_process)

if __name__ == "__main__":
    main()

"""Handles pulling/pushing files from/to cloud.
"""

import argparse
import os
import shutil
import sys
from cloudpathlib import CloudPath


# Mase storage path for local objects;
# mount this directory to see live logging
# and store intermediate results.
LOCAL_FILES_DIR = "/files"


class DecodeFile:
    """
    File class.

    Methods
    -------
    pull: pull the file locally
    push: push the file to remote
    """
    def __init__(self, path: str):
        self.remote_path = path
        self.local_path = path

    def pull(self):
        pass

    def push(self):
        pass


class DecodeFileLocal(DecodeFile):
    pass


class DecodeFileS3(DecodeFile):

    def __init__(self, path: str):
        self.remote_path = path
        self.local_path = os.path.join(LOCAL_FILES_DIR, path.replace("s3://", ""))
        # make sure path exists
        parent_dir = os.path.dirname(self.local_path)
        if not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

    def pull(self):
        cp = CloudPath(self.remote_path)
        if cp.exists():
            cp.download_to(self.local_path)

    def push(self):
        CloudPath(self.remote_path).upload_from(self.local_path)
        if os.path.isfile(self.local_path) or os.path.islink(self.local_path):
            os.remove(self.local_path)
        elif os.path.isdir(self.local_path):
            shutil.rmtree(self.local_path)


def get_file(path: str) -> DecodeFile:
    """
    Get DecodeFile object from path,
    according to type inplicit in the file's path.
    """
    if path.startswith("s3"):
        return DecodeFileS3(path)
    else:
        return DecodeFileLocal(path)


def handle_input_files():
    # Arguments parsing;
    # only handle input path arguments, ignore others
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--model_path')
    if '--train' in sys.argv:
        parser.add_argument('-c', '--calib_path')
        parser.add_argument('-l', '--log_path')
        parser.add_argument('-p', '--param_path')
    if '--fit' in sys.argv:
        parser.add_argument('-e', '--emitter_path')
        parser.add_argument('-f', '--frame_path')
        parser.add_argument('-k', '--frame_meta_path')
    args, _ = parser.parse_known_args()
    args_dict = vars(args)

    # Handle files
    sys_argv = sys.argv[1:]
    for file_path in args_dict.values():
        if file_path:
            file = get_file(file_path)
            # replace remote paths with local paths
            sys_argv = [(file.local_path if arg == file.remote_path else arg) for arg in sys_argv]
            file.pull()
    print(" ".join(sys_argv))


def handle_output_files():
    # Arguments parsing;
    # only handle output path arguments, ignore others
    parser = argparse.ArgumentParser()
    if '--train' in sys.argv:
        #TODO: does this make sense? Do we need the logs on S3,
        # and don't we want to have them available for tensorboard even after training?
        parser.add_argument('-l', '--log_path', default='runs')
        parser.add_argument('-m', '--model_path')
    if '--fit' in sys.argv:
        parser.add_argument('-e', '--emitter_path')
    args, _ = parser.parse_known_args()
    args_dict = vars(args)

    # Handle files
    for file_path in args_dict.values():
        if file_path:
            file = get_file(file_path)
            file.push()


if __name__ == '__main__':

    # Input or output
    input = "--input" in sys.argv
    output = "--output" in sys.argv
    # remove from arguments
    sys.argv = [arg for arg in sys.argv if arg not in ("--input", "--output")]

    if input and not output:
        handle_input_files()
    elif output and not input:
        handle_output_files()
    else:
        raise ValueError("Exactly one of the --input or --output flags has to be set.")

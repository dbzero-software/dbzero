# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import sys

directory = sys.argv[1]
name = sys.argv[2]


def get_directories_and_files(directory):
    dirs = []
    files = []
    for entry in os.scandir(directory):
        if entry.is_dir():
            dirs.append(entry)
        elif entry.is_file():
            files.append(entry)
    return dirs, files
    
def write_object_part(meson_file, cpp_files, name, is_binidng):
    path = "meson.current_source_dir()" if is_binidng else ""
    cpp_files = [f'meson.current_source_dir() / {file}' for file in cpp_files]
    cpp_files = (os.path.join(path, file) for file in cpp_files)
    file_array = "[\n"
    file_array+=",\n".join(cpp_files)
    file_array += ']'
    if is_binidng:
        meson_template = f"""
core_bindings_srcs = {file_array}
        """
    else:
        meson_template = f"""
all_srcs += {file_array}

        """
    meson_file.write(meson_template)


def generate_for_directory(dir, obj_name, is_binding=False):
    genarate_meson_for_parent = False
    with open(os.path.join(dir, "meson.build"), "w") as meson_file:
        dirs, files = get_directories_and_files(dir)
        for subdir in dirs:
            if(generate_for_directory(subdir, "_".join([obj_name, subdir.name]), is_binding )): #  
                meson_file.write(f"subdir('{subdir.name}')\n")
                genarate_meson_for_parent = True
        filenames = []
        for file in files:
            splited = os.path.splitext(file.name)

            if len(splited) == 2 and splited[1] ==".cpp":
                filenames.append(f"'{file.name}'")
                
        if len(filenames):
            write_object_part(meson_file, filenames, obj_name, is_binding)
            genarate_meson_for_parent = True
    return genarate_meson_for_parent

generate_for_directory(directory, name)
import os


def remove_files(path_pattern):
    """ remove the files that fits the pattern

    :param path_pattern: String path pattern
    """
    for f in path_pattern:
        os.remove(f)

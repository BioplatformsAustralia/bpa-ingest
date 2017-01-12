from ...libs.md5lines import md5lines


def parse_md5_file(md5_file):
    data = []
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            print(md5, path)
    return data

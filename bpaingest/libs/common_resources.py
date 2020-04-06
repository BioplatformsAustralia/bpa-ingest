import re
# TODO: there's possibly some redundant escaping here: add tests for this before removing
bsd_md5_re = re.compile(r'^MD5 ?\(([^\)]+)\) ?= ?([0-9a-f]{32})$')
linux_md5_re = re.compile(r'^([0-9a-f]{32}) [\* ]?(.*)$')
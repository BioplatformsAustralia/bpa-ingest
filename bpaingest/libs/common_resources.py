import re

bsd_md5_re = re.compile(r"^MD5 ?\(([^\)]+)\) ?= ?([0-9a-f]{32})$")
linux_md5_re = re.compile(r"^([0-9a-f]{32}) [\* ]?(.*)$")

# -*- coding: utf-8 -*-

# response_stream.py

# Code from:
#   https://gist.github.com/obskyr/b9d4b4223e7eaf4eedcd9defabb34f13

# Original Author:
#   obskyr <powpowd@gmail.com>

# # Streaming a `requests` response as a file
# In many applications, you'd like to access a `requests` response as a file-like object, simply having `.read()`, `.seek()`, and `.tell()` as normal. Especially when you only want to *partially* download a file, it'd be extra convenient if you could use a normal file interface for it, loading as needed.
#
# This is a wrapper class for doing that. Only bytes you request will be loaded - see the example in the gist itself.
#
# ## License
#
# This piece of code is licensed under the Unlicense, which means it is in the public domain; free to use without attribution. Go ahead and use it for anything without worries!
#
# ```
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# For more information, please refer to <https://unlicense.org>
# ```

import requests
from io import BytesIO, SEEK_SET, SEEK_END


class ResponseStream(object):
    def __init__(self, request_iterator):
        self._bytes = BytesIO()
        self._iterator = request_iterator

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        return self

    def _load_all(self):
        self._bytes.seek(0, SEEK_END)
        for chunk in self._iterator:
            self._bytes.write(chunk)

    def _load_until(self, goal_position):
        current_position = self._bytes.seek(0, SEEK_END)
        while current_position < goal_position:
            try:
                current_position += self._bytes.write(next(self._iterator))
            except StopIteration:
                break

    def close(self):
        pass

    def tell(self):
        return self._bytes.tell()

    def read(self, size=None):
        left_off_at = self._bytes.tell()
        if size is None:
            self._load_all()
        else:
            goal_position = left_off_at + size
            self._load_until(goal_position)

        self._bytes.seek(left_off_at)
        return self._bytes.read(size)

    def seek(self, position, whence=SEEK_SET):
        if whence == SEEK_END:
            self._load_all()
        else:
            self._bytes.seek(position, whence)


def main():
    # Use the class by providing a requests stream iterator.
    response = requests.get("http://example.com/", stream=True)
    # Chunk size of 64 bytes, in this case. Adapt to your use case.
    stream = ResponseStream(response.iter_content(64))

    # Now we can read the first 100 bytes (for example) of the file
    # without loading the rest of it. Of course, it's more useful when
    # loading large files, like music images, or video. 😉
    # Seek and tell will also work as expected; important for some applications.
    stream.read(100)


if __name__ == "__main__":
    main()

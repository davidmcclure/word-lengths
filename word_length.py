

import attr
import re


@attr.s
class Text:

    text = attr.ib()

    @classmethod
    def from_file(cls, path):
        with open(path) as fh:
            return cls(fh.read())

    def tokens(self):
        return re.findall('[a-z]+', self.text.lower())

    def token_lengths(self):
        return list(map(len, self.tokens()))

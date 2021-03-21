"""
Copyright (c) 2012-2021 Tim Tomes

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

from urllib.parse import urlparse
import html.parser
import re


def parse_hostname(s):
    host = urlparse(s)
    if not host.scheme:
        host = urlparse("//"+s)
    return host.netloc


def parse_emails(s):
    return re.findall(r"([^\s]+@[^\s]+)", s)


def parse_name(s):
    h = html.parser.HTMLParser()
    elements = [h.unescape(x) for x in s.strip().split()]
    # remove prefixes and suffixes
    names = []
    for i in range(0, len(elements)):
        # preserve initials
        if re.search(r"^\w\.$", elements[i]):
            elements[i] = elements[i][:-1]
        # remove unecessary prefixes and suffixes
        elif re.search(
            r"(?:\.|^the$|^jr$|^sr$|^I{2,3}$)", elements[i], re.IGNORECASE
        ):
            continue
        names.append(elements[i])
    # make sense of the remaining elements
    if len(names) > 3:
        names[2:] = [" ".join(names[2:])]
    # clean up any remaining garbage characters
    names = [re.sub(r"[,"]", "", x) for x in names]
    # set values and return names
    fname = names[0] if len(names) >= 1 else None
    mname = names[1] if len(names) >= 3 else None
    lname = names[-1] if len(names) >= 2 else None
    return fname, mname, lname

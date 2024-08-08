# Copyright (c) 2016-2024 Martin Donath <martin.donath@squidfunk.com>

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

from mkdocs.config.base import Config
from mkdocs.config.config_options import Optional, Type

# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


# Tags plugin configuration
class TagsConfig(Config):
    """
    Configuration class for the Tags plugin.

    This class defines the configuration options for enabling and setting up
    the Tags plugin. It inherits from the `Config` base class.

    Attributes:
        enabled (bool): Indicates whether the Tags plugin is enabled. Defaults to True.
        tags (bool): Controls whether tags are used. Defaults to True.
        tags_file (Optional[str]): Specifies the path to a file containing tags.
                                   If not provided, tags will be managed internally.
    """

    enabled = Type(bool, default=True)

    # Settings for tags
    tags = Type(bool, default=True)
    tags_file = Optional(Type(str))

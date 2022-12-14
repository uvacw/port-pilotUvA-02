class CommandUIRender:
    __slots__ = "page"

    def __init__(self, page):
        self.page = page

    def toDict(self):
        dict = {}
        dict["__type__"] = "CommandUIRender"
        dict["page"] = self.page.toDict()
        return dict


class CommandSystemDonate:
    __slots__ = "key", "json_string"

    def __init__(self, key, json_string):
        self.key = key
        self.json_string = json_string

    def toDict(self):
        dict = {}
        dict["__type__"] = "CommandSystemDonate"
        dict["key"] = self.key
        dict["json_string"] = self.json_string
        return dict

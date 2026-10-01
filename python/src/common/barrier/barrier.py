class Barrier:
    def __init__(self, total):
        self.total = total
        self.arrived = {}

    def arrive(self, query_id, member):
        members = self.arrived.setdefault(query_id, set())
        was_new = member not in members
        members.add(member)
        return was_new

    def is_complete(self, query_id):
        return len(self.arrived.get(query_id, ())) >= self.total

    def forget(self, query_id):
        self.arrived.pop(query_id, None)

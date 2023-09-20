from holoanalytics.utils import importing


SESSION_PATH = None
starting_data = importing.import_member_data()
groups_branches_units = importing.get_groups_branches_units(starting_data)


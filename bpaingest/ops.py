import ckanapi


def ckan_method(ckan, object_type, method):
    return getattr(ckan.action, object_type + '_' + method)


def update_or_create(ckan, object_type, data):
    try:
        ckan_method(ckan, object_type, "show")(id=data['name'])
    except ckanapi.errors.NotFound:
        ckan_method(ckan, object_type, "create")(name=data['name'])
    return ckan_method(ckan, object_type, "patch")(id=data['name'], **data)

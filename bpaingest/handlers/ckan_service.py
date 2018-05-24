from datetime import datetime
import json
import os

from bpaingest.handlers.common import RequestsSession, UnrecoverableError, json_ckan_converter, set_up_credentials


def set_up_ckan_service(env):
    credentials = set_up_credentials(env)
    session = RequestsSession(default_timeout=env.ckan_timeout)
    return CKANService(session, credentials=credentials, base_url=env.ckan_base_url)


class CKANURLs:
    def __init__(self, base_url):
        self.base = base_url
        self.actions = os.path.join(self.base, 'api/3/action')

        self.packages_with_resources = self.action_url('current_package_list_with_resources')
        self.package_search = self.action_url('package_search')
        self.resource = self.action_url('resource_show')
        self.resource_patch = self.action_url('resource_patch')

    def action_url(self, action):
        return os.path.join(self.actions, action)


class CKANService:
    PACKAGE_LIMIT = 100

    def __init__(self, session, credentials, base_url):
        self.session = session
        self.credentials = credentials
        self.base_url = base_url
        self.urls = CKANURLs(base_url)

    @property
    def auth_header(self):
        return {'Authorization': self.credentials['CKAN_API_KEY']}

    @property
    def auth_admin_header(self):
        return {'Authorization': self.credentials['CKAN_ADMIN_API_KEY']}

    def get_packages_by_bpa_id(self, bpa_id):
        params = {
            'include_private': True,
            'q': 'bpa_id:%s' % bpa_id,
        }
        resp = self.session.get(self.urls.package_search, headers=self.auth_header, params=params)
        try:
            resp.raise_for_status()
            json_resp = resp.json()
            if not json_resp['success']:
                raise Exception('Package search (by bpa_id) returned success False')
            return json_resp['result']['results']
        except Exception as exc:
            msg = 'Package search (%s) for packages with bpa_id "%s" was NOT successful!' % (
                resp.request.url, bpa_id)
            raise Exception(msg) from exc

    def get_all_resources(self):
        next_page = 1
        while True:
            packages = self._get_next_packages(next_page)
            for p in packages:
                for r in p.get('resources', ()):
                    yield r
            if len(packages) < self.PACKAGE_LIMIT:
                break
            next_page += 1

    def get_resource_by_id(self, resource_id):
        resp = self.session.get(
            self.urls.resource,
            headers=self.auth_header, params={'id': resource_id})
        try:
            resp.raise_for_status()
            json_resp = resp.json()
            if not json_resp['success']:
                raise Exception('Resource show returned success False')
            return json_resp['result']
        except Exception as exc:
            msg = 'Resource show (%s) for resource "%s" was NOT successful! ' % (
                resp.request.url, resource_id)
            raise Exception(msg) from exc

    def get_resource_etag(self, url):
        resp = self.session.head(url, headers=self.auth_header)
        resp.raise_for_status()
        etag = resp.headers.get('ETag', '').strip('"')
        if not etag:
            raise UnrecoverableError('ETag header missing for URL %s' % url)
        return etag

    def patch_resource(self, data_dict):
        json_data = json.dumps(data_dict, default=json_ckan_converter)
        headers = self.auth_admin_header
        headers.update({'Content-Type': 'application/json'})
        resp = self.session.post(self.urls.resource_patch, headers=headers, data=json_data)
        resp.raise_for_status()
        return resp.json()

    def mark_resource_passed_integrity_check(self, resource_id):
        return self.patch_resource(dict(
            id=resource_id, s3_etag_verified_at=datetime.utcnow()))

    def _get_next_packages(self, page, limit=PACKAGE_LIMIT):
        resp = None
        try:
            resp = self.session.get(
                self.urls.packages_with_resources,
                headers=self.auth_header,
                params={'limit': limit, 'page': page})
            resp.raise_for_status()
            json_resp = resp.json()
            if not json_resp['success']:
                raise Exception('Get package list with resourses call returned success False')
            return json_resp.get('result', ())
        except Exception as exc:
            msg = 'Get package list with resources call (%s) was NOT successful! ' % (resp.request.url if resp else self.urls.packages_with_resources)
            print(msg)
            raise Exception(msg) from exc

import hashlib
import logging
import uuid
from urllib.request import urlopen
import traceback
from collections import Counter

from ckan import logic
from ckan import model
from ckan import plugins as p
from ckan.common import config
from ckan.model import Session
from ckan.logic import get_action


from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from ckan.lib.munge import munge_tag
from ckan.lib.munge import munge_title_to_name
from ckan.lib.search import rebuild


from ckan.lib.search.index import PackageSearchIndex
from ckan.lib.helpers import json
from ckan.lib.navl.validators import not_empty


import oaipmh.client
from oaipmh.metadata import MetadataRegistry

from ckanext.oaipmh.metadata import oai_ddi_reader
from ckanext.oaipmh.metadata import oai_dc_reader

log = logging.getLogger(__name__)


class DataVerseHarvester(HarvesterBase, SingletonPlugin):
    '''
    Harvester per Dataverse
    GATHER: makes a request to the index service and saves each entry in a HarvestObject
    FETCH:  read the HarvestObject, retrieve the metadata, update the content of the HarvestObject by adding the newly uploaded metadata
    IMPORT: parses the HarvestObject and creates / updates the corresponding dataset
    '''

# implements(IHarvester)
#
# _user_name = "harvest"
#
# source_config = {}
#
    def info(self):
        """
         Return information about this harvester.
         """
        return {
         "name": "Dataverse Harvester",
         "title": "Dataverse Harvester",
         "description": "Harvester for Dataverse",
        }

    def harvester_name(self):
        return "Dataverse Harvester"
#
# def attach_resources(self, metadata, package_dict):
#     raise NotImplementedError
#
# ## IHarvester
#
# def validate_config(self, source_config):
#     try:
#         source_config_obj = json.loads(source_config)
#
#         if 'id_field_name' in source_config_obj:
#             if not isinstance(source_config_obj['id_field_name'], str):
#                 raise ValueError('"id_field_name" should be a string')
#         else:
#             raise KeyError("Cannot process configuration not identifying 'id_field_name'.")
#
#         if 'filter' in source_config_obj:
#             if not isinstance(source_config_obj['filter'], str):
#                 raise ValueError('"filter" should be a string')
#
#     except ValueError as e:
#         raise e
#     log.debug(source_config)
#
#     return source_config
#
#def _set_config(self, url):
 #   """ return name, descriptions and subjects """

  #  subject_str = self.source_config.get('subject')
   # final_url = f'{url}/api/search?q=*&type=dataset&fq=subject_ss:{subject_str}&per_page=1000'
    # q = * & type = dataset & fq = subject_ss:Chemistry & metadata_fields = citation: *

    #log.info(f'Retrieving data from URL {url}')
    #request = urlopen(final_url)
#    content = request.read()

#    json_content = json.loads(content)

#    datakey = json_content.get('data')
#    items = datakey['items']
#    ret = []
#    guids = []
#    log.debug(type(items))

  #  for item in items:
#
#         each_itemData = Counter(item)
#
#         doc_id = item.get(self.source_config['id_field_name'])
#         # This info below is about the Data found in each dataset
#         # log.info(f'Data: found {name} {description} {subjects}')
#         temp_dict = dict(each_itemData)
#         temp_dict.update({'guid': doc_id})
#         ret.append(dict(temp_dict))
#
#         guids.append(doc_id)
#     return guids, ret
#
# def gather_stage(self, harvest_job):
#     log = logging.getLogger(__name__ + '.gather')
#     log.debug(f'{self.harvester_name()} gather_stage for job: {harvest_job}')
#     # Get source URL
#     url = harvest_job.source.url
#
#     log.debug("in gather stage: %s" % harvest_job.source.url)
#     self._set_source_config(harvest_job.source.config)
#
#     try:
#         local_guids, data = self._set_config(url)
#     except Exception as e:
#         self._save_gather_error(f'Error harvesting {self.harvester_name()}: {harvest_job}')
#         return None
#
#     query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
#         filter(HarvestObject.current == True). \
#         filter(HarvestObject.harvest_source_id == harvest_job.source.id)
#     guid_to_package_id = {}
#
#     for guid, package_id in query:
#         guid_to_package_id[guid] = package_id
#
#     guids_in_db = set(guid_to_package_id.keys())
#
#     guids_in_harvest = set(local_guids)
#
#     new = guids_in_harvest - guids_in_db
#     delete = guids_in_db - guids_in_harvest
#     change = guids_in_db & guids_in_harvest
#
#     ids = []
#     for guid in new:
#         doc = dict()
#         for d in data:
#             if d['global_id'] == guid:
#               doc = json.dumps(d)
#               break
#
#         obj = HarvestObject(
#             guid=guid, job=harvest_job, content=doc,
#             extras=[HOExtra(key='status', value='new')])
#
#         log.debug(obj)
#         obj.save()
#         ids.append(obj.id)
#
#     for guid in change:
#         doc = dict()
#         for d in data:
#             if d['global_id'] == guid:
#                 doc = json.dumps(d)
#                 break
#         obj = HarvestObject(guid=guid, job=harvest_job, content=doc,
#                             package_id=guid_to_package_id[guid],)
#                             #extras=[HOExtra(key='status', value='change')])
#
#         obj.save()
#         ids.append(obj.id)
#
#     for guid in delete:
#         obj = HarvestObject(guid=guid, job=harvest_job,
#                             package_id=guid_to_package_id[guid],)
#                             #extras=[HOExtra(key='status', value='delete')])
#         ids.append(obj.id)
#         model.Session.query(HarvestObject). \
#             filter_by(guid=guid). \
#             update({'current': False}, False)
#         obj.save()
#
#     if len(ids) == 0:
#         self._save_gather_error(f'No records received from the {self.harvester_name()} service {harvest_job}')
#         return None
#
#     return ids
#
# def fetch_stage(self, harvest_object):
#     return True
#
# def import_stage(self, harvest_object):
#
#     log = logging.getLogger(__name__ + '.import')subject
#     log.debug(f'{self.harvester_name()}: Import stage for harvest object: {harvest_object.id}')
#
#     if not harvest_object:
#         log.error('No harvest object received')
#         return False
#
#     if not harvest_object.content:
#         log.error('Harvest object contentless')
#         self._save_object_error(
#             f'Empty content for object {harvest_object.id}',
#             harvest_object,
#             'Import'
#         )
#         return False
#
#     # self._set_source_config(harvest_object.source.config)
#
#     status = self._get_object_extra(harvest_object, 'status')
#
#
#     # Get the last harvested object (if any)
#     previous_object = Session.query(HarvestObject) \
#         .filter(HarvestObject.guid == harvest_object.guid) \
#         .filter(HarvestObject.current == True) \
#         .first()
#
#     context = {'model': model, 'session': model.Session, 'user': self._get_user_name()}
#
#     if status == 'delete':
#         # Delete package
#         p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
#         log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))
#
#         return True
#
#     # Flag previous object as not current anymore
#     if previous_object:
#         previous_object.current = False
#         previous_object.add()
#subject
#     # Flag this object as the current one
#     harvest_object.current = True
#     harvest_object.add()
#
#     # Generate GUID if not present (i.e. it's a manual import)
#     if not harvest_object.guid:
#         self._save_object_error('Missing GUID for object {0}'
#                                 .format(harvest_object.id), harvest_object, 'Import')
#         return False
#
#     ## pre-check to skip resource logic in case no changes occurred remotely
#     try:
#        if status == 'change':
#
#            # Check if the document has changed
#            m = hashlib.md5()
#            m.update(previous_object.content)
#            old_md5 = m.hexdigest()
#
#            m = hashlib.md5()
#            m.update(harvest_object.content)
#            new_md5 = m.hexdigest()
#
#
#            if old_md5 == new_md5:
#
#                # Assign the previous job id to the new object to # avoid losing history
#                harvest_object.harvest_job_id = previous_object.job.id
#                harvest_object.add()
#
#                harvest_object.metadata_modified_date = previous_object.metadata_modified_date
#                harvest_object.add()
#
#                # Delete the previous object to avoid cluttering the object table
#                previous_object.delete()
#
#                # Reindex the corresponding package to update the reference to the harvest object
#                context.update({'validate': False, 'ignore_auth': True})
#                try:
#                    package_dict = logic.get_action('package_show')(context,
#                                                                    {'id': harvest_object.package_id})
#                except p.toolkit.ObjectNotFound:
#                    pass
#                else:
#                    for extra in package_dict.get('extras', []):
#                        if extra['key'] == 'harvest_object_id':
#                            extra['value'] = harvest_object.id
#                    if package_dict:
#                        package_index = PackageSearchIndex()
#                        package_index.index_package(package_dict)
#
#                log.info(f'{self.harvester_name()} document with GUID {harvest_object.id} unchanged, skipping...')
#                model.Session.commit()
#
#                return True
#     except (TypeError) as e:
#        log.exception(e)
#        pass
#
#     # Build the package dict
#     package_dict = {}
#     content = json.loads(harvest_object.content)
#     log.debug(content)
#
#     # package_dict, metadata = self.create_package_dict(harvest_object.guid, harvest_object.content)
#
#     package_dict["id"] = munge_title_to_name(harvest_object.guid)
#     package_dict["name"] = package_dict["id"]
#
#     mapping = self._get_mapping()
#     for ckan_field, dataverse_field in mapping.items():
#         try:
#             package_dict[ckan_field] = content[dataverse_field]
#         except (IndexError, KeyError):
#             continue
#
#     if not package_dict:
#         log.error('No package dict returned, aborting import for object {0}'.format(harvest_object.id))
#         return False
#
#
#     # We need to get the owner organization (if any) from the harvest source dataset
#     source_dataset = model.Package.get(harvest_object.source.id)
#     if source_dataset.owner_org:
#         package_dict['owner_org'] = source_dataset.owner_org
#
#     #self.attach_resources(metadata, package_dict)
#
#     # Create / update the package
#     try:
#         context = {'model': model,
#                    'session': model.Session,
#                    'user': "harvest",
#                    'extras_as_string': True,
#                    'api_version': '2',
#                    'return_id_only': True}
#
#
#         # The default package schema does not like Upper case tags
#         #tag_schema = logic.schema.default_tags_schema()
#         ##tag_schema['name'] = [not_empty, unicode]
#         #
#         #if status == 'new':
#         #    package_schema = logic.schema.default_create_package_schema()
#         #    package_schema['tags'] = tag_schema
#         #    context['schema'] = package_schema
#         #
#         #    # We need to explicitly provide a package ID, otherwise ckanext-spatial
#         #    # won't be be able to link the extent to the package.
#         #    package_dict['id'] = unicode(uuid.uuid4())
#         #    package_schema['id'] = [unicode]
#         #
#         #    # Save reference to the package on the object
#         #    harvest_object.package_id = package_dict['id']
#         #    harvest_object.add()
#         #    # Defer constraints and flush so the dataset can be indexed with
#         #    # the harvest object id (on the after_show hook from the harvester
#         #    # plugin)
#         #    Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
#         #    model.Session.flush()
#         #    self._create_or_update_package(
#         #        package_dict, harvest_object, "package_show"
#         #    )
#         #
#         #    try:
#         #        package_id = p.toolkit.get_action('package_create')(context, package_dict)
#         #        log.info(f'{self.harvester_name()}: Created new package {package_id} with guid {harvest_object.guid}')
#         #    except p.toolkit.ValidationError as e:
#         #        self._save_object_error(f'Validation Error: {e.error_summary} {harvest_object} Import')
#         #        return False
#         #
#         #elif status == 'change':
#         #    # we know the internal document did change, bc of a md5 hash comparison done above
#         #
#         #    package_schema = logic.schema.default_update_package_schema()
#         #    package_schema['tags'] = tag_schema
#         #    context['schema'] = package_schema
#         #
#         #    package_dict['id'] = harvest_object.package_id
#         #    try:
#         #        package_id = p.toolkit.get_action('package_update')(context, package_dict)
#         #        log.info(f'{self.harvester_name()} updated package {package_id} with guid {harvest_object.guid}')
#         #    except p.toolkit.ValidationError as e:
#         #        self._save_object_error(f'Validation Error: {e.error_summary} {harvest_object} Import')
#         #        return False
#
#         log.debug("Create/update package using dict: %s" % package_dict)
#         self._create_or_update_package(
#             package_dict, harvest_object, "package_show"
#         )
#         rebuild(package_dict["name"])
#
#         model.Session.commit()
#         log.debug("Finished record")
#
#
#     except Exception as e:
#                 log.exception(e)
#                 self._save_object_error(
#                     "Exception in fetch stage for %s: %r / %s"
#                     % (harvest_object.guid, e, traceback.format_exc()),
#                     harvest_object,
#                 )
#                 return False
#
#     return True
#
# def _set_source_config(self, config_str):
#     '''
#     Loads the source configuration JSON object into a dict for
#     convenient access
#     '''
#     if config_str:
#         self.source_config = json.loads(config_str)
#         log.debug(f'{self.harvester_name()} Using config: {self.source_config}')
#     else:
#         self.source_config = {}
#
# def _get_mapping(self):
#
#     return {
#         "title": "name",
#         "notes": "description",
#         "maintainer": "publisher",
#         "type": "type",
#         "url": "url",
#     }
#
# #def _extract_author(self, content):
#  #   return ", ".join(content["authors"])
#
# def _get_object_extra(self, harvest_object, key):
#     '''
#     Helper function for retrieving the value from a harvest object extra,
#     given the key
#     '''
#     for extra in harvest_object.extras:
#         if extra.key == key:
#             return extra.value
#     return None
#
# def _get_user_name(self):
#     '''
#     Returns the name of the user that will perform the harvesting actions
#     (deleting, updating and creating datasets)
#     By default this will be the internal site admin user. This is the
#     recommended setting, but if necessary it can be overridden with the
#     `ckanext.spatial.harvest.user_name` config option, eg to support the
#     old hardcoded 'harvest' user:
#        ckanext.spatial.harvest.user_name = harvest
#     '''
#     if self._user_name:
#         return self._user_name
#
#     self._site_user = p.toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
#
#     config_user_name = config.get('ckanext.spatial.harvest.user_name')
#     if config_user_name:
#         self._user_name = config_user_name
#     else:
#         self._user_name = self._site_user['name']
#
#     return self._user_name
#
    def gather_stage(self, harvest_job):
        """
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
              stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
              the guid and a reference to its source and job.
            - creating and storing any suitable HarvestGatherErrors that may
              occur.
            - returning a list with all the ids of the created HarvestObjects.
        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        """
        log.debug("in gather stage: %s" % harvest_job.source.url)
        try:
            harvest_obj_ids = []
            registry = self._create_metadata_registry()
            self._set_config(harvest_job.source.config)
            client = oaipmh.client.Client(
                harvest_job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get,
            )

            client.identify()  # check if identify works
            for header in self._identifier_generator(client):
                harvest_obj = HarvestObject(
                    guid=header.identifier(), job=harvest_job
                )
                # TODO: drop
                # if harvest_obj.guid != '10.14272/VIZKKYMOUGQEKZ-UHFFFAOYSA-L.1':
                # continue
                harvest_obj.save()
                harvest_obj_ids.append(harvest_obj.id)
                log.debug("Harvest obj %s created" % harvest_obj.id)
                # TODO: drop
                # return harvest_obj_ids
        except (HTTPError) as e:
            log.exception(
                "Gather stage failed on %s (%s): %s, %s"
                % (harvest_job.source.url, e.fp.read(), e.reason, e.hdrs)
            )
            self._save_gather_error(
                "Could not gather anything from %s" % harvest_job.source.url,
                harvest_job,
            )
            return None
        except (Exception) as e:
            log.exception(
                "Gather stage failed on %s: %s"
                % (
                    harvest_job.source.url,
                    str(e),
                )
            )
            self._save_gather_error(
                "Could not gather anything from %s: %s / %s"
                % (harvest_job.source.url, str(e), traceback.format_exc()),
                harvest_job,
            )
            return None
        log.debug(
            "Gather stage successfully finished with %s harvest objects"
            % len(harvest_obj_ids)
        )
        return harvest_obj_ids

    def _identifier_generator(self, client):
        """
        pyoai generates the URL based on the given method parameters
        Therefore one may not use the set parameter if it is not there
        """
        if self.set_spec:
            for header in client.listIdentifiers(
                    metadataPrefix=self.md_format, set=self.set_spec
            ):
                yield header
        else:
            for header in client.listIdentifiers(
                    metadataPrefix=self.md_format
            ):
                yield header

    def _create_metadata_registry(self):
        registry = MetadataRegistry()
        registry.registerReader("oai_dc", oai_dc_reader)
        registry.registerReader("oai_ddi", oai_ddi_reader)
        return registry

    def _set_config(self, source_config):
        try:
            config_json = json.loads(source_config)
            log.debug("config_json: %s" % config_json)
            try:
                username = config_json["username"]
                password = config_json["password"]
                self.credentials = (username, password)
            except (IndexError, KeyError):
                self.credentials = None

            self.user = "harvest"
            self.set_spec = config_json.get("set", None)
            self.md_format = config_json.get("metadata_prefix", "oai_dc")
            self.force_http_get = config_json.get("force_http_get", False)

        except ValueError:
            pass

    def fetch_stage(self, harvest_object):
        """
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
              perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.
        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        """
        content = None
        log.debug("in fetch stage: %s" % harvest_object.guid)
        try:
            self._set_config(harvest_object.job.source.config)
            registry = self._create_metadata_registry()
            client = oaipmh.client.Client(
                harvest_object.job.source.url,
                registry,
                self.credentials,
                force_http_get=self.force_http_get,
            )
            record = None
            try:
                log.debug(
                    "Load %s with metadata prefix '%s'"
                    % (harvest_object.guid, self.md_format)
                )

                self._before_record_fetch(harvest_object)

                record = client.getRecord(
                    identifier=harvest_object.guid,
                    metadataPrefix=self.md_format,
                )
                self._after_record_fetch(record)
                log.debug("record found!")
            except:
                log.exception("getRecord failed for %s" % harvest_object.guid)
                self._save_object_error(
                    "Get record failed for %s!" % harvest_object.guid,
                    harvest_object,
                )
                return False

            header, metadata, _ = record

            try:
                metadata_modified = header.datestamp().isoformat()
            except:
                metadata_modified = None
                
            #log.debug("Header data elemt  %s", header.element() )
            #log.debug("metadata  subject %s" ,metadata.getMap())

            ''' To Fetch only chemistry metadata from Dublin Core Subject. We fetch everything from using OAI-DC, then search for the subject.
            If _Chemistry_ inside the array has an hit, then harvest only that DOI's metadata '''


            content_dict = metadata.getMap()
            log.debug("Subject are %s ", content_dict['subject'])

            for subject in content_dict['subject']:
                try:
                    if subject == 'Chemistry':
                        content_dict["set_spec"] = header.setSpec()
                        if metadata_modified:
                            content_dict["metadata_modified"] = metadata_modified
                        log.debug(content_dict)
                        content = json.dumps(content_dict)
                        harvest_object.content = content
                        harvest_object.save()
                        log.debug("Only Chemistry metadata is dumped")
                        break

                    else:
                        log.info("Not chemistry metadata for %s", harvest_object.guid)
                        return False

                except:
                        log.exception("Dumping the metadata failed!")
                        self._save_object_error(
                            "Dumping the metadata failed!", harvest_object
                        )
                        return False


        except (Exception) as e:
            log.exception(e)
            self._save_object_error(
                "Exception in fetch stage for %s: %r / %s"
                % (harvest_object.guid, e, traceback.format_exc()),
                harvest_object,
            )
            return False

        return True

    def _before_record_fetch(self, harvest_object):
        pass

    def _after_record_fetch(self, record):
        pass

    def import_stage(self, harvest_object):
        """
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g
              create a CKAN package).
              Note: if this stage creates or updates a package, a reference
              to the package must be added to the HarvestObject.
              Additionally, the HarvestObject must be flagged as current.
            - creating the HarvestObject - Package relation (if necessary)
            - creating and storing any suitable HarvestObjectErrors that may
              occur.
            - returning True if everything went as expected, False otherwise.
        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        """

        log.debug("in import stage: %s" % harvest_object.guid)
        if not harvest_object:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received")
            return False

        try:
            self._set_config(harvest_object.job.source.config)
            context = {
                "model": model,
                "session": Session,
                "user": self.user,
                "ignore_auth": True,
            }

            package_dict = {}
            content = json.loads(harvest_object.content)
            log.debug(content)

            package_dict["id"] = munge_title_to_name(harvest_object.guid)
            package_dict["name"] = package_dict["id"]

            mapping = self._get_mapping()
            for ckan_field, oai_field in mapping.items():
                try:
                    package_dict[ckan_field] = content[oai_field][0]
                except (IndexError, KeyError):
                    continue

            # add author
            package_dict["author"] = self._extract_author(content)

            # add owner_org
            source_dataset = get_action("package_show")(
                context.copy(), {"id": harvest_object.source.id}
            )
            owner_org = source_dataset.get("owner_org")
            package_dict["owner_org"] = owner_org

            # add license
            package_dict["license_id"] = self._extract_license_id(content)

            # add resources
            url = self._get_possible_resource(harvest_object, content)
            package_dict["resources"] = self._extract_resources(url, content)

            # extract tags from 'type' and 'subject' field
            # everything else is added as extra field
            tags, extras = self._extract_tags_and_extras(content)
            package_dict["tags"] = tags
            package_dict["extras"] = extras

            # groups aka projects
            groups = []

            # create group based on set
            if content["set_spec"]:
                log.debug("set_spec: %s" % content["set_spec"])
                groups.extend(
                    {"id": group_id}
                    for group_id in self._find_or_create_groups(
                        content["set_spec"], context.copy()
                    )
                )

            # add groups from content
            groups.extend(
                {"id": group_id}
                for group_id in self._extract_groups(content, context.copy())
            )

            package_dict["groups"] = groups

            # allow sub-classes to add additional fields
            package_dict = self._extract_additional_fields(
                content, package_dict
            )

            log.debug("Create/update package using dict: %s" % package_dict)
            self._create_or_update_package(
                package_dict, harvest_object, "package_show"
            )
            rebuild(package_dict["name"])
            Session.commit()

            log.debug("Finished record")
        except (Exception) as e:
            log.exception(e)
            self._save_object_error(
                "Exception in fetch stage for %s: %r / %s"
                % (harvest_object.guid, e, traceback.format_exc()),
                harvest_object,
            )
            return False
        return True

    def _get_mapping(self):
        return {
            "title": "title",
            "notes": "description",
            "maintainer": "publisher",
            "maintainer_email": "maintainer_email",
            "url": "source",
        }

    def _extract_author(self, content):
        return ", ".join(content["creator"])

    def _extract_license_id(self, content):
        return ", ".join(content["rights"])

    def _extract_tags_and_extras(self, content):
        extras = []
        tags = []
        for key, value in content.items():
            if key in self._get_mapping().values():
                continue
            if key in ["type", "subject"]:
                if type(value) is list:
                    tags.extend(value)
                else:
                    tags.extend(value.split(";"))
                continue
            if value and type(value) is list:
                value = value[0]
            if not value:
                value = None
            if key.endswith("date") and value:
                # the ckan indexer can't handle timezone-aware datetime objects
                try:
                    from dateutil.parser import parse

                    date_value = parse(value)
                    date_without_tz = date_value.replace(tzinfo=None)
                    value = date_without_tz.isoformat()
                except (ValueError, TypeError):
                    continue
            extras.append({"key": key, "value": value})

        tags = [{"name": munge_tag(tag[:100])} for tag in tags]

        return (tags, extras)

    def _get_possible_resource(self, harvest_obj, content):
        url = None
        candidates = content["identifier"]
        candidates.append(harvest_obj.guid)
        for ident in candidates:
            if ident.startswith("http://") or ident.startswith("https://"):
                url = ident
                break
        return url

    def _extract_resources(self, url, content):
        resources = []
        log.debug("URL of resource: %s" % url)
        if url:
            try:
                resource_format = content["format"][0]
            except (IndexError, KeyError):
                resource_format = "HTML"
            resources.append(
                {
                    "name": content["title"][0],
                    "resource_type": resource_format,
                    "format": resource_format,
                    "url": url,
                }
            )
        return resources

    def _extract_groups(self, content, context):
        if "series" in content and len(content["series"]) > 0:
            return self._find_or_create_groups(content["series"], context)
        return []

    def _extract_additional_fields(self, content, package_dict):
        # This method is the ideal place for sub-classes to
        # change whatever they want in the package_dict
        return package_dict

    def _find_or_create_groups(self, groups, context):
        log.debug("Group names: %s" % groups)
        group_ids = []
        for group_name in groups:
            data_dict = {
                "id": group_name,
                "name": munge_title_to_name(group_name),
                "title": group_name,
            }
            try:
                group = get_action("group_show")(context.copy(), data_dict)
                log.info("found the group " + group["id"])
            except:
                group = get_action("group_create")(context.copy(), data_dict)
                log.info("created the group " + group["id"])
            group_ids.append(group["id"])

        log.debug("Group ids: %s" % group_ids)
        return group_ids

    #def _get_json_content(self, identifiers):


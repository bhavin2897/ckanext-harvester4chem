import json
from dateutil.parser import parse

import logging
import os.path
import random
from urllib.error import HTTPError
import traceback
import datetime
from datetime import datetime
import requests

from urllib.parse import urlparse, parse_qs

from ckan.model import Session
from ckan.logic import get_action
from ckan import model

from ckanext.rdkit_visuals.models.molecule_tab import Molecules as molecules
from ckanext.rdkit_visuals.models.molecule_rel import MolecularRelationData as mol_rel_data

from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.lib.munge import munge_tag
from ckan.lib.munge import munge_title_to_name
from ckan.lib.search import rebuild
from ckanext.harvest.model import HarvestObject

from rdkit.Chem import inchi
from rdkit.Chem import rdmolfiles
from rdkit.Chem import Draw
from rdkit.Chem import Descriptors

import requests

log = logging.getLogger(__name__)


class ChemotionRepoHarvester(HarvesterBase):
    """
        To get the datasets from Chemotion Repository using BioSchemas via Swagger API, we use this Harvester class.
        This Harvester provides IDs and dataset's metadata are attained by two different APIs.

        The source configuration must contain the Parameters dictionary w.r.t Swagger API
     """

    def info(self):
        """
        Return information about this harvester.
        """
        return {
            "name": "Chemotion repo Harvester",
            "title": "Chemotion Swagger Harvester",
            "description": "Harvester for scrapping and harvesting metadata from Chemotion-Repo via  Swagger API",
        }

    def gather_stage(self, harvest_job):
        """
        Gathering inchiKeys from each dataset at the given time and from & to.
        Gather is to gather,
        :param harvest_job: HarvestJob object
        :return: A list of HarvestObject ids for Fetch stage
        """

        global type_chem, offset, limit, date_from, date_to

        log.debug("in gather stage: %s" % harvest_job.source.url)
        harvest_obj_ids = []

        try:
            type_chem, offset, limit, date_from, date_to = self._set_config(harvest_job.source.config)
            log.debug(type_chem, offset, limit, date_from, date_to)
        except Exception as e:
            log.error(e)

        # type_chem = 'Container'
        # offset = 0
        # limit = 1000
        # date_from = '2024-07-01'
        # date_to = datetime.date.today()

        base_swagger_api = harvest_job.source.url + '/publications'
        log.debug("%s" % base_swagger_api)

        for identi in self._get_dataseturl(base_url=base_swagger_api, type_chem=type_chem, offset=offset,
                                           limit=limit, date_from=date_from, date_to=date_to):
            harvest_obj = HarvestObject(guid=identi, job=harvest_job)
            harvest_obj.save()
            harvest_obj_ids.append(harvest_obj.id)

            log.debug("Harvest obj %s created" % harvest_obj.id)

        return harvest_obj_ids

    def fetch_stage(self, harvest_object):
        """
        TODO: Extend this information
        Fetch scrapable text from the URL/ harvest job

        :return:
        """

        base_swagger_api = harvest_object.source.url

        type_chem = "Container"

        try:

            log.debug("in fetch stage: %s" % harvest_object.guid)

            base_url = base_swagger_api + f'/download_json?{type_chem}&id=0&inchikey={harvest_object.guid}'

            response = requests.get(base_url)
            response.raise_for_status()  # Raises an error for 4XX/5XX responses

            metadata_response = requests.get(base_url)
            metadata_response.raise_for_status()
            metadata_data = metadata_response.json()

            content = json.dumps(metadata_data)

            harvest_object.content = content
            harvest_object.save()

            return True

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return False

    def import_stage(self, harvest_object):
        """
        To harvest data from api metadata to ckan database
        :param harvest_object: HarvestObject object
        :return: True if everything went well, False if errors were found
        """

        global created, modified, technique_measure
        if not harvest_object:
            log.error("No harvest object received")
            self._save_object_error("No harvest object received")
            return False

        try:
            # self._set_config(harvest_object.job.source.config,harvest_object.job.source.frequency)
            context = {
                "model": model,
                "session": Session,
                "user": 'harvest',
                "ignore_auth": True,
            }

            package_dict = {}

            content = json.loads(harvest_object.content)
            log.debug("in import stage %s" % harvest_object.guid)
            log.debug(content)  # Occupying to much, space and time

            # get id
            package_dict["inchi_key"] = munge_title_to_name(harvest_object.guid)
            package_dict["id"] = munge_title_to_name(content["@id"])

            log.debug(f"Here is the package id saved {package_dict['id']}")

            package_dict['name'] = content['@id']

            package_dict["title"] = content['name']
            package_dict['url'] = content['url']

            # add owner org
            source_dataset = get_action("package_show")(
                context.copy(), {"id": harvest_object.source.id}
            )
            owner_org = source_dataset.get("owner_org")
            package_dict["owner_org"] = owner_org

            # add resources
            package_dict["resources"] = self._extract_resources(content)
            package_dict["language"] = 'english'
            package_dict["maintainer"] = content['includedInDataCatalog']['name']

            # add notes, license_id
            try:
                package_dict['notes'] = content['description']
            except KeyError as e:
                log.exception(f'description not available {e}')
                package_dict['notes'] = ''
                pass
            try:
                package_dict["license_id"] = self._extract_license_id(context=context, content=content)
                log.debug(f'This is the license {package_dict["license_id"]}')
            except Exception as e:
                log.exception(f'License Error: {e}')
                pass

            # self._extract_extras_image(package=package_dict, content_hasBioPart=content)

            # Chemical information by extracting BioChemEntity

            try:
                self._extract_extras_image(package=package_dict, content_hasBioPart=content)
                content_about = content['isPartOf']['about']
                content_hasBioPart = content_about[0]['hasBioChemEntityPart']

                package_dict['inchi'] = content_hasBioPart['inChI']
                package_dict['inchi_key'] = content_hasBioPart['inChIKey']
                # smiles = content_hasBioPart['smiles']
                # package_dict['smiles'] = next((item for item in smiles_a if item is not None), 'n/a')
                package_dict['smiles'] = content_hasBioPart['smiles']
                package_dict['exactmass'] = content_hasBioPart['molecularWeight']['value']
                package_dict['mol_formula'] = content_hasBioPart['molecularFormula']
                package_dict['iupacName'] = content_hasBioPart['iupacName']

            except KeyError:
                # when the inchi is not present, then it takes empty dict.
                # Should find something better solution
                content_hasBioPart = {}
                package_dict['inchi'] = harvest_object.guid
                package_dict['inchi_key'] = ''

                pass

            try:
                # measurement Technical Information
                technique_measure = content['measurementTechnique']

                if isinstance(technique_measure, dict):

                    technique = technique_measure['name']
                    package_dict['measurement_technique'] = technique
                    package_dict['measurement_technique_iri'] = technique_measure['@id']

                elif isinstance(technique_measure, str):
                    package_dict['measurement_technique'] = technique_measure

                else:
                    log.error("Measurement not available for package %s", package_dict['id'])

            except (KeyError, TypeError) as e:
                log.exception(f'TypeError or KeyError for MeasurementTechnique for ID{package_dict["id"]}: {str(e)}')
            pass

            try:
                # Obtaining DOI from @id of Content
                package_dict['doi'] = content['@id']
            except KeyError as e:
                log.exception(f'DOI not found {e}')
                pass

            try:
                # Date of metadata publication
                package_dict['metadata_published'] = content['isPartOf']['datePublished']
            except KeyError as e:
                log.exception(f'Metadata date Published Error: {e}')

            # Author information
            try:
                # double_dict_ispartof = content['isPartOf']['isPartOf']
                # package_dict['metadata_published'] = double_dict_ispartof['datePublished']
                # citation_author = double_dict_ispartof['citation']

                author_list = content['author']

                for author in author_list:
                    author_all = str()
                    author_all += author['name']
                    package_dict['author'] = author_all

                # Remove the trailing semicolon and space
                # package_dict['author'] = package_dict['author'].strip('; ')

                log.debug(package_dict['author'])

            except Exception as e:
                log.exception(f'Author/date_published Error {e}')
                pass

            package_dict['variableMeasured'] = self._extract_variable_measured(content=content)

            tags = self._extract_tags(content)
            package_dict['tags'] = tags

            # creating package
            log.debug("Create/update package using dict: %s" % package_dict)
            self._create_or_update_package(
                package_dict, harvest_object, "package_show"
            )

            rebuild(package_dict["name"])
            Session.commit()

            self._send_to_db(package=package_dict, content=content_hasBioPart)

            log.debug("Finished record")

        except Exception as e:
            log.exception(e)
            self._save_object_error(
                "Exception in fetch stage for %s: %r / %s"
                % (harvest_object.guid, e, traceback.format_exc()),
                harvest_object,
            )
            return False
        return True

    def _set_config(self, source_config):

        """
        Configuration from GUI is being added here.
        This function also checks which frequency with which harvest process would be proceeded.

        It also checks weather the date and time mentioned and acts accordingly

        :param source_config: Configuration from the GUI. It should only take type_chem, offset, limit, date_from, date_to
        which are required for the Swagger API to run

        :return all the required values for the Swagger API to run

        """
        now = datetime.now()

        try:
            config_json = json.loads(source_config)
            log.debug("config_json: %s" % config_json)

            type_chem = config_json.get("type_chem")
            offset = config_json.get("offset")
            limit = config_json.get("limit")
            date_from = config_json.get("date_from")
            date_to = config_json.get("date_to")

            if limit is None:
                limit = int(1000)

            if date_from is None:
                date_from = '1971-01-01'

            if date_to is None:
                date_to = now

            return type_chem, offset, limit, date_from, date_to

        except ValueError as e:
            log.error(f"Configuration Error {e}")
            pass

    def _get_dataseturl(self, base_url, type_chem, offset, limit, date_from, date_to):
        """
        :param base_url: receives url, which is a Swagger-API url of chemotion - repo only
        :param type_chem: Here we currently using Container only for retrieving Datasets!! We can also use it for Sample
        :param offset: starting Offset
        :param limit: max.1000 which will iterate
        :param date_from: date from which retrieving
        :param date_to: date to which retrieving
        :return: List of InChIKeys present in Chemotion-Repo for the given Parameters
        """
        all_data = []
        inchikey_list = []

        while True:
            base_url = base_url + f'?type={type_chem}&offset={offset}&limit={limit}&date_from={date_from}&date_to={date_to}'

            response = requests.get(base_url)
            response.raise_for_status()
            data = response.json()
            publication_data = data['publications']

            all_data.extend(publication_data)

            if not publication_data:
                break

            # Increase the offset for the next batch
            offset += limit

        log.debug(f'Total InChIKey Gathered: {len(all_data)}')

        for url in all_data:
            # Parse the URL
            parsed_url = urlparse(url)

            # Extract the query parameters
            query_params = parse_qs(parsed_url.query)

            # Get the inchikey value
            inchikey = query_params.get('inchikey', [None])[0]
            inchikey_list.append(inchikey)

        return inchikey_list

    def _get_mapping(self):
        return {
            "title": "name",
            "notes": "description",
            "url": "url",
            "metadata_modified": "dateModified",
            "metadata_created": "dateCreated",
        }

    def _extract_resources(self, content):
        resources = []
        url = content['url']
        log.debug("URL of resource: %s" % url)
        if url:
            try:
                resource_format = content["format"][0]
            except (IndexError, KeyError):
                resource_format = "HTML"
            resources.append(
                {
                    "name": content["name"],
                    "resource_type": resource_format,
                    "format": resource_format,
                    "url": url,
                }
            )
        return resources

    def _extract_tags(self, content):
        try:
            # Safely retrieve 'measurementTechnique' and ensure it's a dictionary
            technique_measure_tag = content.get('measurementTechnique', {})
            if not isinstance(technique_measure_tag, dict):
                raise TypeError("Expected 'measurementTechnique' to be a dictionary.")

            # Extract 'name' from 'technique_measure_tag', ensuring it exists and is a string
            technique = technique_measure_tag.get('name', '')
            if not isinstance(technique, str):
                raise TypeError("Expected 'name' to be a string.")

            # Prepare and return tags
            tags = [{"name": munge_tag(technique[:100])}]
            return tags

        except KeyError:
            log.exception('KeyError: Measurement Technique field is missing')
            return []

        except TypeError as e:
            log.exception(f'TypeError: {str(e)}')
            return []

    def _extract_variable_measured(self, content):

        variable_measured_package_list = []

        from curies import Converter as Converter

        converter_instance = Converter.from_prefix_map(
            {
                "CHEBI": "http://purl.obolibrary.org/obo/CHEBI_",
                "MONDO": "http://purl.obolibrary.org/obo/MONDO_",
                "GO": "http://purl.obolibrary.org/obo/GO_",
                "FIX": "http://purl.obolibrary.org/obo/FIX_",
                "OBI": "http://purl.obolibrary.org/obo/OBI_",
                "NCIT": "http://purl.obolibrary.org/obo/NCIT_",
                "CHMO": "http://purl.obolibrary.org/obo/CHMO_",
            }
        )

        for values in content.get('variableMeasured', []):  # Safe access to 'variableMeasured'
            variable_measured_dict = {
                'variableMeasured_name': values.get('name', ''),
                'variableMeasured_propertyID': values.get('propertyID', ''),
                'variableMeasured_value': values.get('value', ''),
                # 'variableMeasured_tsurl': converter_instance.expand(values.get('propertyID', '')),
            }

            log.debug(f'Variable Measured: {variable_measured_dict}')

            variable_measured_package_list.append(variable_measured_dict)

        log.debug(f"All Variable Measured: {variable_measured_package_list}")

        return variable_measured_package_list

    def _extract_extras_image(self, package, content_hasBioPart):
        extras = []
        package_id = package['id']
        #
        content_about = content_hasBioPart['isPartOf']['about']
        content = content_about[0]['hasBioChemEntityPart']
        #
        standard_inchi = content['inChI']
        #
        inchi_key = content['inChIKey']

        if standard_inchi.startswith('InChI'):
            molecu = inchi.MolFromInchi(standard_inchi)
            log.debug("Molecule generated")
            try:
                filepath = '/var/lib/ckan/default/storage/images/' + str(inchi_key) + '.png'
                if os.path.isfile(filepath):
                    log.debug("Image Already exists")
                else:
                    Draw.MolToFile(molecu, filepath)
                    log.debug("Molecule Image generated for %s", package_id)

            except Exception as e:
                log.error(e)
                pass
        else:
            pass

        # extracting date metadata as extra data.
        try:
            if content['datePublished']:
                published = content['datePublished']
                date_value = parse(published)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "datePublished", "value": value})
            if content['dateCreated']:
                created = content['dateCreated']
                date_value = parse(created)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "dateCreated", "value": value})
            if content['dateModified']:
                modified = content['dateModified']
                date_value = parse(modified)
                date_without_tz = date_value.replace(tzinfo=None)
                value = date_without_tz.isoformat()
                extras.append({"key": "dateModified", "value": value})
        except Exception:
            pass

        log.debug(f"Data saved to extras {extras}")
        # return extras

        return None

    def _extract_license_id(self, context, content):
        package_license = None
        content_license = content['license']
        license_list = get_action('license_list')(context.copy(), {})
        for license_name in license_list:

            if content_license == license_name['id'] or content_license == license_name['url'] or content_license == \
                    license_name['title']:
                package_license = license_name['id']

        return package_license

    def _send_to_db(self, package, content):

        """
        sends the molecule information and all other information to database directly.
        """

        package_id = package['id']

        try:
            standard_inchi = content['inChI']
            inchi_key = content['inChIKey']
            smiles = content['smiles']
            log.debug(f'here smiles looks like when harvester SMILES: {smiles}')
            # smiles = next((item for item in smiles_a if item is not None), 'n/a')
            exact_mass = content['molecularWeight']['value']
            mol_formula = content['molecularFormula']

            # Check if the row already exists, if not then INSERT
            molecule_id = molecules._get_inchi_from_db(inchi_key)
            log.debug(f"Current molecule_d  {molecule_id}")
            relation_value = mol_rel_data.get_mol_formula_by_package_id(package_id)
            log.debug(f"Here is the relation {relation_value}")

            if not molecule_id:  # if there is no molecule at all, it inserts rows into molecules and molecule_rel_data dt
                molecules.create(standard_inchi, smiles, inchi_key, exact_mass, mol_formula)
                new_molecules_id = molecules._get_inchi_from_db(inchi_key)
                new_molecules_id = new_molecules_id[0]
                # Check if relaionship exists
                log.debug(f"New molecule {new_molecules_id}")
                mol_rel_data.create(new_molecules_id, package_id)
                log.debug('data sent to molecules and relation db')

            elif not relation_value:  # if the molecule exists, but the relation doesn't exist, it create the relation
                # with molecule ID
                log.debug("Relationship must be created")
                mol_rel_data.create(molecule_id[0], package_id)
                log.debug('data sent to mol_relation db')
            else:  # if the both exists
                log.debug('Nothing to insert. Already existing')

        except Exception as e:
            if e:
                log.error(f'Sent to db not possible because of this error {e}')
                pass
            else:
                pass
        return 0

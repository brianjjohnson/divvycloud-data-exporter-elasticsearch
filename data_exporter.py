from DivvyDb.document_store import get_document_store, DocumentNotFoundError
from DivvyJobs import schedules
from DivvyJobs.jobs import JobQueue, SimpleJobTemplate
from DivvyJobs.queues.redis import RedisQueue
from DivvyPlugins.hookpoints import hookpoint
from DivvyPlugins.plugin_helpers import register_job_module, unregister_job_module
from DivvyPlugins.plugin_jobs import PluginJob
from DivvyRQ.DivvyRQ import DivvyQueue
from DivvyResource.Resources.base import get_class_for_type,get_all_classes
from DivvyInterfaceMessages import ResourceConverters
from DivvySession.DivvySession import EscalatePermissions
from DivvyUtils import schedule
from DivvyUtils.field_definition import SelectionChoice

from DivvyDb.DivvyCloudGatewayORM import DivvyCloudGatewayORM
from DivvyDb.DivvyDb import SharedSessionScope, NewSessionScope
from DivvyPlugins.plugin_metadata import PluginMetadata
from DivvyPlugins.settings import GlobalSetting, CachedResourceSetting
from DivvyResource.Resources import DivvyPlugin,Instance, LoadBalancer
import logging
import dateutil.parser


class metadata(PluginMetadata):
    """
    Information about this plugin
    """
    version = '1.0'
    last_updated_date = '2015-10-02'
    author = 'Divvy Cloud Corp.'
    nickname = 'ElasticSearch Exporter'
    default_language_description = 'Data Exporter for ElasticSearch'
    support_email = 'support@divvycloud.com'
    support_url = 'http://support.divvycloud.com'
    main_url = 'http://www.divvycloud.com'
    category = 'Data Exporter'
    managed = True

# Prefix for settings and properties used by this plugin
_SETTING_PROPERTY_PREFIX = DivvyPlugin.get_current_plugin().name

# Global settings that are saved
_SETTING_RESOURCE_LIST            = '%s.export_list' % (_SETTING_PROPERTY_PREFIX)
_SETTING_ENABLED            = '%s.enabled' % (_SETTING_PROPERTY_PREFIX)

setting_resource_selection = GlobalSetting(name=_SETTING_RESOURCE_LIST,display_name='Select Resources to export',type_hint='json_list',description='Select Resources to Export',choices=[SelectionChoice(str(cls)) for cls in  get_all_classes()])
setting_enabled = GlobalSetting(name=_SETTING_ENABLED,display_name='Enabled exporter',type_hint='bool',description='Enabled data exporter',default_value='false')
#
# @hookpoint('divvycloud.resource.modified')
# def export_on_modified(resource, old_resource, user_resource_id=None):
#     if(ResourceExporter.is_selected_resource_type(resource)):
#         exporter = ResourceExporter()
#         converted_resource = exporter.convert_resource(resource)
#         exporter.export_resource(converted_resource)
#
#
#
# @hookpoint('divvycloud.resource.created')
# def export_on_created(resource, user_resource_id=None):
#     if(ResourceExporter.is_selected_resource_type(resource)):
#         exporter = ResourceExporter()
#         converted_resource = exporter.convert_resource(resource)
#         exporter.export_resource(converted_resource)

class ResourceExporter(PluginJob):
    def __init__(self):
        super(ResourceExporter, self).__init__()

    @EscalatePermissions()
    @SharedSessionScope(DivvyCloudGatewayORM)
    def get_selected_resource_types(self):
        selected_resource_types = []
        selected_resource_types_str = setting_resource_selection.get_for_resource(DivvyPlugin.get_current_plugin())
        for resource_type_str in selected_resource_types_str:
            if not(resource_type_str in [cls.get_resource_type().lower() for cls in get_all_classes()]):
                logging.getLogger("DataExporter").error("%s is not a valid resources, skipping" % (resource_type_str))
                continue
            selected_resource_types.append(get_class_for_type(resource_type_str))

        return selected_resource_types

    @EscalatePermissions()
    @SharedSessionScope(DivvyCloudGatewayORM)
    def run(self,resource=None):
        if not(resource):
            self.convert_all_resources()
        else:
            converted_resource = self.convert_resource(resource)
            self.export_resource(converted_resource)

    @SharedSessionScope(DivvyCloudGatewayORM)
    def convert_resource(self,resource):
        converted_resource = ResourceConverters.convert_resource(resource)
        if(isinstance(resource,Instance)):
            converted_resource.instance.instance_type = converted_resource.instance.instance_type.replace(".","_")


        return converted_resource


    @SharedSessionScope(DivvyCloudGatewayORM)
    def convert_all_resources(self):
        for resource_type in self.get_selected_resource_types():
            logging.getLogger("DataExporter").info('Exporting %s to ElasticSearch' % (str(resource_type.get_resource_type())))
            converted_resources = {}
            for resource in resource_type.list():
                single_converted_resource = self.convert_resource(resource)
                converted_resources[resource.get_resource_id().to_string()] =  single_converted_resource

            self.export_resources(resource_type.get_resource_type(),converted_resources)


    def export_resources(self,index,resource_dict):
        logging.getLogger('elasticsearch').setLevel(logging.ERROR)
        storage = get_document_store('default',index_name=index)
        storage.put_documents(resource_dict)
        return

    def export_resource(self,index,resource_dict):
        storage = get_document_store('default',index_name=index)
        storage.put_document(resource_dict)
        return


def list_job_templates():
    """
    Rapid job scheduler job template list method
    """
    # Only 1 job template for this job
    return [SimpleJobTemplate(job_class=ResourceExporter,
                              schedule_goal=schedules.LazyScheduleGoal(queue_name='DivvyCloudProcessors', schedulable=schedule.Periodic(minutes=5)),
                              template_id='processor-dataexporter')]


_JOB_LOADED = False
def load():
    global _JOB_LOADED
    try:
        _JOB_LOADED = register_job_module(__name__)
    except AttributeError:
        pass

def unload():
    global _JOB_LOADED
    if _JOB_LOADED:
        unregister_job_module(__name__)
        _JOB_LOADED = False



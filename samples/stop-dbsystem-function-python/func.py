import io
import json
import logging
import oci

from fdk import response

def get_dbs_state(dbs_client, dbs_id):
    try:
        get_dbs_response = dbs_client.get_db_system(dbs_id)
    except oci.exceptions.ServiceError as e:
        logging.info('Get DBS state failed. {0}'.format(e))
        raise
    logging.info('DBS state: {}'.format(get_dbs_response.data.lifecycle_state))
    return get_dbs_response.data.lifecycle_state

def stop_dbs(dbs_client, dbs_id):
    logging.info('Stopping Instance: {}'.format(dbs_id))
    try:
        if get_dbs_state(dbs_client, dbs_id) in 'ACTIVE':
            try:
                stop_dbs_response = dbs_client.stop_db_system(dbs_id,
                                                              oci.mysql.models.StopDbSystemDetails(shutdown_type='FAST'))
            except oci.exceptions.ServiceError as e:
                logging.info('Stopping instance failed. {0}'.format(e))
                raise
        else:
            logging.info('The instance was in the incorrect state to stop' .format(dbs_id))
            raise
    except oci.exceptions.ServiceError as e:
        logging.info('Stopping instance failed. {0}'.format(e))
        raise
    logging.info('Stopped Instance: {}'.format(dbs_id))
    return get_dbs_state(dbs_client, dbs_id)

def handler(ctx, data: io.BytesIO = None):
    signer = oci.auth.signers.get_resource_principals_signer()
    try:
        body = json.loads(data.getvalue())
        resourceName = body["data"]["resourceName"]
        resourceId = body["data"]["resourceId"]
        eventType = body["eventType"]
        source = body["source"]
        logging.info('***eventType:' + eventType + ' resourceName:' + resourceName)
    except Exception as e:
        print('Error receiving JSON input:', e, flush=True)
        # logging.info('Error receiving JSON input. {0}'.format(e))
        raise

    dbs_client = oci.mysql.DbSystemClient(config={}, signer=signer)

    resp = stop_dbs(dbs_client, resourceId)

    return response.Response(
        ctx,
        response_data=json.dumps({"status": "{0}".format(resp)}),
        headers={"Content-Type": "application/json"}
    )

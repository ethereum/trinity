import logging

from eth_utils import humanize_hash

logger = logging.getLogger("validator_client.publisher")
logger.setLevel(logging.DEBUG)


async def publisher(context, signatory, beacon_node):
    async for duty, signature in signatory.signatures():
        logger.info("got signature %s for duty %s", humanize_hash(signature), duty)
        await beacon_node.publish(duty, signature)

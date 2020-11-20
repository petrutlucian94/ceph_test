import sys

from os_brick.initiator import connector

if len(sys.argv) != 2:
	print("Usage: python %s <image_name>", sys.argv[0])
	exit(1)

image = sys.argv[1]
connection_properties = dict(name=image)

connector  = connector.InitiatorConnector.factory(
    protocol='rbd',
    root_helper=None,
    device_scan_attempts=1,
    device_scan_interval=1,
    do_local_attach=True)

print("Attaching volume: %s." % connection_properties)
attachment = connector.connect_volume(connection_properties)
print("Attached %s to %s." % (image, attachment['path']))

print("Getting volume path.")
volume_paths = connector.get_volume_paths(connection_properties)
print("Volume paths: %s" % volume_paths)

print("Disconnecting volume")
connector.disconnect_volume(connection_properties)
print("Successfully disconnected volume.")

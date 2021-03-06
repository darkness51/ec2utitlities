import logging
import glob
import shlex
import time
from argparse import ArgumentParser
from subprocess import Popen, PIPE
from os import path, pardir

logger = logging.getLogger("Raid")

class Raid(object):
    """
    Configure a RAID in Amazon EC2
    """
    def __init__(self, devices_pattern):
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        # create a formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        # Add handler to logger
        logger.addHandler(ch)
        logger.setLevel(logging.DEBUG)

        self._devices_pattern = devices_pattern

        self.system_initial_config()
        # Remove EC2 default /mnt from fstab
        self._fstab = ''
        self._file_to_open = '/etc/fstab'
        self.run_command('sudo chmod 777 {0}'.format(self._file_to_open))

        with open(self._file_to_open, 'r') as f:
            for line in f:
                if not "/mnt" in line:
                    self._fstab += line

        with open(self._file_to_open, 'w') as f:
            f.write(self._fstab)

        self.run_command('sudo chmod 644 {0}'.format(self._file_to_open))

        # Create a list of devices
        self._devices = glob.glob("/dev/{0}*".format(self._devices_pattern))
        self._devices.remove('/dev/{0}a1'.format(self._devices_pattern))
        self._devices.sort()

        if len(self._devices) > 1:
            mount_point = self.mount_raid()
        
    def mount_raid(self):
        logger.info('Clear "invalid flag 0x0000 of partition table 4" by issuing a write, then running fdisk on each device...')
        format_commands = "echo 'n\np\n1\n\n\nt\nfd\nw'"
        for device in self._devices:
            logger.info('Confirming devices are not mounted:')
            logger.info("Unmounting the device {0}".format(device))
            self.run_command('sudo umount {0}'.format(device))
            self.run_command("echo 'w' | sudo fdisk -c -u {0}".format(device))
            self.run_command("{0} | sudo fdisk -c -u {1}".format(format_commands, device))
        
        # Create a list of partitions to RAID
        self.run_command("sudo fdisk -l")
        partitions = glob.glob('/dev/{0}*[0-9]'.format(self._devices_pattern))
        partitions.remove('/dev/{0}a1'.format(self._devices_pattern))
        partitions.sort()
        logger.info('Partitions about to be added to RAID0 set: {0}'.format(partitions))

        # Make sure the partitions are umounted and create a list string
        partion_list = ''
        for partition in partitions:
            logger.info('Confirming partitions are not mounted:')
            self.run_command('sudo umount {0}'.format(partition))
        partion_list = ' '.join(partitions).strip()

        logger.info('Creating the RAID0 set:')
        time.sleep(3) # was at 10

        # Continuously create the Raid device, in case there are errors
        raid_created = False
        while not raid_created:
            logger.info("Running command: 'sudo mdadm --create /dev/md0 --chunk=256 --level=0 --raid-devices={0} {1}'".format(len(partitions), partion_list))
            self.run_command("echo 'y' | sudo mdadm --create /dev/md0 --chunk=256 --level=0 --raid-devices={0} {1}".format(len(partitions), partion_list))
            logger.info("is raid created: {0}".format(raid_created))
            raid_created = True
            logger.info("is raid created: {0}".format(raid_created))

            self.run_command('echo DEVICE {0} | sudo tee /etc/mdadm/mdadm.conf'.format(partion_list))
            time.sleep(5)

            # New parsing and elimination of the name= field due to 12.04's new RAID'ing methods
            response = self.run_command('sudo mdadm --examine --scan')
            logger.info("response: {0}".format(response))
            response = ' '.join(response.split(' ')[0:-1])
            logger.info("response: {0}".format(response))
            with open('/etc/mdadm/mdadm.conf', 'a') as f:
                f.write(response)
            self.run_command('sudo update-initramfs -u')

            time.sleep(10)
            self.run_command('sudo blockdev --setra 512 /dev/md0')

            logger.info('Formatting the RAID0 set:')
            time.sleep(10)
            raidError = False
            self.run_command('sudo mkfs.xfs -f /dev/md0')

            if raidError:
                self.run_command('sudo mdadm --stop /dev/md_d0')
                self.run_command('sudo mdadm --zero-superblock /dev/sdb1')
                raid_created = False

        # Configure fstab and mount the new RAID0 device
        mnt_point = '/raid0'
        self.run_command("echo '/dev/md0\t{0}\txfs\tdefaults,nobootwait,noatime\t0\t0' | sudo tee -a /etc/fstab".format(mnt_point))

        logger.info('Showing RAID0 details:')
        self.run_command('cat /proc/mdstat')
        self.run_command('echo "15000" > /proc/sys/dev/raid/speed_limit_min')
        self.run_command('sudo mdadm --detail /dev/md0')
        return mnt_point

    def system_initial_config(self):
        self.run_command('echo "* soft nofile 32768" | sudo tee -a /etc/security/limits.conf')
        self.run_command('echo "* hard nofile 32768" | sudo tee -a /etc/security/limits.conf')
        self.run_command('echo "root soft nofile 32768" | sudo tee -a /etc/security/limits.conf')
        self.run_command('echo "root hard nofile 32768" | sudo tee -a /etc/security/limits.conf')

    def run_command(self, command):
        """ Run system command """
        
        command = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
        (out, errors) = command.communicate()
        logger.info(out)
        if errors:
            logger.error(errors)

        return out


if __name__ == '__main__':
    raid = Raid("xvd")

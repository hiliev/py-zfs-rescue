all:
	@echo "make d   : generate raid-z1 testdisks disk[0-2].bin"
	@echo "make lo  : setup disk[0-2].bin in /dev/loop[0-2]"
	@echo "make lou : teardown /dev/loop[0-2]"
	@echo "make runfiles  : run py-zfs-rescue against disk[0-2].bin"
	@echo "make runserver : run py-zfs-rescue against block server"

d:
	sudo sh gen_disks.sh

lo: lou
	if [ ! -f disk0.bin ]; then echo "Run 'make d' to generate disk[0-2]..bin"; exit 1; fi
	sudo losetup /dev/loop0 disk0.bin
	sudo losetup /dev/loop1 disk1.bin
	sudo losetup /dev/loop2 disk2.bin

lou: 
	-sudo losetup -d /dev/loop0
	-sudo losetup -d /dev/loop1
	-sudo losetup -d /dev/loop2

runfiles:
	bash test_files.sh

runserver:
	bash test_server.sh

.PHONY: d lo lou files 


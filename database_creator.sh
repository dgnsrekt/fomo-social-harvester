#!/bin/sh
sudo su - postgres << END_OF_SCRIPT
dropdb --echo --if-exists fomo_social_harvester_db;
createdb --echo --owner='postgres' --encoding='UTF8' --tablespace='pg_default' --lc-collate='en_US.UTF-8' --lc-ctype='en_US.UTF-8' fomo_social_harvester_db;

END_OF_SCRIPT

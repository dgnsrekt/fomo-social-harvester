#!/bin/sh
sudo cp fomo_social_harvester_db.dump /var/lib/postgresql/fomo_social_harvester_db.dump

sudo su - postgres << END_OF_SCRIPT
psql fomo_social_harvester_db < fomo_social_harvester_db.dump

END_OF_SCRIPT

sudo rm /var/lib/postgresql/fomo_social_harvester_db.dump

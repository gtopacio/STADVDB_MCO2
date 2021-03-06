CREATE USER 'backend'@'%' IDENTIFIED BY '12345';
CREATE USER 'replicator'@'%' IDENTIFIED BY '12345';

GRANT ALL PRIVILEGES ON *.* TO 'backend'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'replicator'@'%';

FLUSH PRIVILEGES;

USE mco2;
BEGIN;
CREATE TABLE `temp` LIKE `movies`;
INSERT INTO `temp` (`id`, `name`, `year`, `rank`, `lastUpdated`) SELECT `id`, `name`, `year`, `rank`, lastUpdated FROM movies WHERE `year` < 1980 OR `year` IS NULL;
RENAME TABLE `movies` TO `old`, `temp` TO `movies`;
DROP TABLE `old`;
COMMIT;
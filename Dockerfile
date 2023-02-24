FROM php:7.1-cli

RUN apt-get update
RUN apt-get install -y unzip

RUN php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');"
RUN php composer-setup.php
RUN php -r "unlink('composer-setup.php');"
RUN mv composer.phar /usr/local/bin/composer

WORKDIR /app

COPY . .

RUN composer update --prefer-source --no-interaction


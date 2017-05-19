#!/bin/sh

lein clean
lein cljsbuild once min
rsync -a resources/public/ /home/conrad/Dropbox/btwtest/conrad/static/public/
cache_value=`date +%s%N`
cat explorer_template.html | sed -e "s/CACHE_VALUE/$cache_value/g" > /home/conrad/Dropbox/btwtest/conrad/templates/explorer.html

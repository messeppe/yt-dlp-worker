#!/bin/sh
npx --yes bgutil-ytdlp-pot-provider serve &
sleep 3
exec python -u worker.py

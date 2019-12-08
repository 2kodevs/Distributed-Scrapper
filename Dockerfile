# Using image in `python-image`
FROM 2kodevs:scrapper-deps

# Adding project files
ADD ./ /home/Distributed-Scrapper

# Moving inside the project folder
WORKDIR /home/Distributed-Scrapper

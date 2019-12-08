# using image in `python-image`
FROM 2kodevs:scrapper-deps

# adding project files
ADD . /app/

# moving inside the project folder
WORKDIR /app

# v7files

v7files is a WebDAV server backed by MongoDB GridFS file storage.

It is written in Java, using the Jetty embedded web server and the Milton WebDAV library.

All file contents are stored in GridFS, but the file (and folder) metadata is [stored in separate collection](https://github.com/thiloplanz/v7files/wiki/StorageFormat),
where it can also be versioned. Identical content is only stored once, even if more than one file refers (or used to refer) to it, 
and content can also be compressed using zip compression (good for text files) or delta storage (good for files that are similar to others).

You can configure [Authentication](https://github.com/thiloplanz/v7files/wiki/Authentication), and set [access permissions](https://github.com/thiloplanz/v7files/wiki/Authorisation) 
(separately for read and write) for every file and folder. A file that does not have its own set of permissions inherits them from its parent folder.

## Online documentation

For more, detailed, and up-to-date information, see [the project wiki](https://github.com/thiloplanz/v7files/wiki/).

## Usage

Start the program from the command line:

     # java -jar v7files.jar serve

This will start a WebDAV server on port 8080.

You can optionally specify a properties file with configuration.

    # java -jar v7files.jar serve -f config.properties

All configuration options can also be set (or overridden) by system properties

    # java -jar v7files.jar serve -Dv7files.endpoints=/webdav -Dhttp.port=5555


## Configuration

Take a look at [the file that defines the default settings](https://github.com/thiloplanz/v7files/blob/master/src/main/resources/v7db/files/defaults.properties).

For a quick-start, you probably want to set the following properties:

    # The name of the database to connect to 
    mongo.db = test
    
    # enable authentication using the following two users
    auth.provider = demo
    auth.demo.user.admin.password = admin
    auth.demo.user.demo.password = demo
    # important: if you don't want these two users enabled, 
    # you must explicitly set their passwords to blank

    # allow anonymous access
    auth.anonymous = anonymous

    # set global access permissions
    acl.provider = global
    acl.read = anonymous, users
    acl.write = admins




 



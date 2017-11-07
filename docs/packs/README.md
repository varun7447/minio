Introduction [![Slack](https://slack.minio.io/slack?type=svg)](https://slack.minio.io)
------------

This feature allows Minio to serve collection of disks greater than 16 in a distributed setup. There are no special configuration changes required to enable this feature. Access to files stored across this setup are locked and synchornized by default.

Motivation
----------

As next-generation data centers continue to shrink, IT professions must reevaluate ahead to get the benefits of greater server density and storage density. Computer hardware is changing rapidly in system form factors, virtualization, containerization have allowed far more enterprise computing with just a fraction of the physical space. Increased densities allow for smaller capital purchases and lower energy bills.

Restrictions
------------

* Each slot still has a maximum of 16 disks requirement, you can start with multiple such packs statically.
* Static packs set of disks and cannot be changed, there is no elastic expansion allowed.
* Static packs set of disks and cannot be changed, there is no elastic removal allowed.
* ListObjects() across packs can be relatively slower since List happens on all servers, and is merged at this layer.

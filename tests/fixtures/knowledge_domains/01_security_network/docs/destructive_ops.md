# Data lifecycle — destructive changes

## Sanctions and cleanup

1. Destructive operations MUST purge user data in the tenant slice before `rm -rf` cleanup of scratch volumes.
2. Operators MUST NOT drop production databases without a verified backup.
3. Emergency wipe procedures MAY remove all cached secrets after a token leak is confirmed.

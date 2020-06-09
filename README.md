# cp-nlenhertscholer
cp-nlenhertscholer created by GitHub Classroom

Link to Site: https://nlenhertscholer.ucmpcs.org/

## Notes on changed config files in web directory
I was experiencing issues during autoscaling. GUNICORN workers were timing out and not able to retrieve an HTTPS request,
so the website was not working. To alleviate this, I added a `--timeout` flag in run_gas.sh and set it to 120 (this can be seen in the .env file). This was able to fix the issue, and now instances are provisioned in a healthy state.

## Archive Process
For the archive process, I have 1 SNS (nlenhertscholer_archive) that the web-app send a message to once a job is completed.
An SQS is subscribed to this topic, and has a message retrieval delay of 5 minutes. This would be enough for the user,
and then after ~5 min, the message is received and can be archived.
First the archive retrieval gets the user profile data, and if they are a free user, it proceeds to download the log file. The file is read into memory, and then passed to the 
vault so it can be archived. Once done, the file is deleted, and the archive id is passed
to the database so it can be retrieved. I did it this way because it felt like the most straight forward way to do it. Amazon has a delay message timer so I utilized that, and didn't rely on my code to handle that. This helps decouple the front end from the back end even more.

## Restore Process
Here, once the user subscribes, an SNS message is sent so that the restoration of all the files can proceed.  The restore.py file gets the message from the corresponding SQS and gets a list of all of the users files. For each item, if it has an archive id, it starts an archive retrieval job. It also subscribes another SNS that Glacier can send a message to when the object is done being retrieved. It also uses a
try/except block to attempt expedited retrieval and if that fails, try normal retrieval.

Once it is done being retrieved, the thaw.py file polls for messages of done files. It then extracts the information from the message and uploads the file to S3. Once successful, it deletes the file from glacier and removes the archive id from the database.

I did it in this manner because it again decouples jobs that might take a long time. Once a retrieval is started, it might take a few hours, and the same file shouldn't be stuck waiting for it. This two-tiered SNS-SQS systems allows for that to happen smoothly.
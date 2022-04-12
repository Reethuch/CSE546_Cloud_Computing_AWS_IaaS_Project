# Image recognition on cloud using IAAS services of AWS

  Used EC2, S3, and SQS services of Amazon Cloud. Created a simple website which allows multiple user to input images. The website is Flask application. The input images is put into S3 input bucket and id is given to the SQS to take the image from the S3 bucket. Based on the length of the queue we scale-out or scale in the number of instances. We set a threshold of 19 instances. The image is processed on the instance and the result of the output is stored in the S3 output bucket.
  

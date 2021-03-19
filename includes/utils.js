function unloadToS3(query, bucket_dir) {
  return `UNLOAD('${query}') TO 's3://paidpal-dataloader/${bucket_dir}' iam_role 'arn:aws:iam::382977655890:role/RedShiftS3-FA' CSV`;
}
module.exports = {
  unloadToS3
};

function refreshrange(refreshrange) {
    return  
    `(CURRENT_DATE(), INTERVAL -30 DAY)`
    ;
}
module.exports = {
   refreshrange 
   };







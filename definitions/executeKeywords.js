const input = vars.config;

for (var i = 0; i < input.length; i++) {
  operate(`keywords_${input[i].site}`).queries(
    executeKeywords(
      input[i]['keywords']['bucket_dir'],
      input[i]['schemas'],
      input[i]['site']));
    // `select ${input[i]['site']}`);
}

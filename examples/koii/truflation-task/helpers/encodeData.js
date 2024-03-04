const { getAlloc } = require('./getAlloc');

function encodeData(type, fields) {
  const allocLength =
    type.layout.span >= 0 ? type.layout.span : getAlloc(type, fields);

  const data = Buffer.alloc(allocLength);

  const layoutFields = Object.assign({ instruction: type.index }, fields);

  type.layout.encode(layoutFields, data);

  return data;
}

module.export = encodeData;
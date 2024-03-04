// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getAlloc(type, fields) {
  let alloc = 0;
  type.layout.fields.forEach(item => {
    if (item.span >= 0) {
      alloc += item.span;
    } else if (typeof item.alloc === 'function') {
      alloc += item.alloc(fields[item.property]);
    }
  });
  return alloc;
}

module.export = getAlloc;

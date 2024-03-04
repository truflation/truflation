
function makeChunks(data, size) {
    const numBytes = data.length;
  
    const numChunks = Math.ceil(numBytes / size);
  
    const c = [];
  
    let i = 0;
    for (; i < numChunks - 1; i++) {
      const sInd = i * size;
      const eInd = (i + 1) * size;
      c.push(data.slice(sInd, eInd));
    }
    c.push(data.slice(i * size, numBytes));
  
    return c;
  }

  module.export = makeChunks;
  
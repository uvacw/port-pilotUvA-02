let pyScript

onmessage = (event) => {
  const { eventType } = event.data
  switch (eventType) {
    case 'initialise':
      initialise().then(() => {
        self.postMessage({ eventType: 'initialiseDone' })
      })
      break

    case 'firstRunCycle':
      pyScript = self.pyodide.runPython(`port.start(${event.data.sessionId})`)
      runCycle(null)
      break

    case 'nextRunCycle':
      const { response } = event.data
      unwrap(response).then((userInput) => {
        runCycle(userInput)
      })
      break

    default:
      console.log('[ProcessingWorker] Received unsupported event: ', eventType)
  }
}

function runCycle (payload) {
  console.log('[ProcessingWorker] runCycle ' + JSON.stringify(payload))
  scriptEvent = pyScript.send(payload)
  self.postMessage({
    eventType: 'runCycleDone',
    scriptEvent: scriptEvent.toJs({
      create_proxies: false,
      dict_converter: Object.fromEntries
    })
  })
}

function unwrap (response) {
  console.log('[ProcessingWorker] unwrap response: ' + JSON.stringify(response.payload))
  return new Promise((resolve) => {
    switch (response.payload.__type__) {
      case 'PayloadFile':
        copyFileToPyFS(response.payload.value, resolve)
        break

      default:
        resolve(response.payload)
    }
  })
}

function getRandomInt(max) {
  return Math.floor(Math.random() * (max + 1));
}

function copyFileToPyFS(file, resolve) {
  directory = `/file-input${getRandomInt(1000000)}`
  try {
    self.pyodide.FS.mkdir(directory)
  } catch (error) {
    console.log("MKDIR ERROR")
    throw error
  }
  try {
    self.pyodide.FS.mount(
      self.pyodide.FS.filesystems.WORKERFS,
      {
        files: [file]
      },
      directory
    )
  } catch(error) {
    console.log("MY ERROR")
    console.log(error)
  }
  resolve({ __type__: 'PayloadString', value: directory + "/" + file.name })
}

//function copyFileToPyFS (file, resolve) {
//  const reader = file.stream().getReader()
//  const pyFile = self.pyodide.FS.open(file.name, 'w')
//
//  const writeToPyFS = ({ done, value }) => {
//    if (done) {
//      resolve({ __type__: 'PayloadString', value: file.name })
//    } else {
//      self.pyodide.FS.write(pyFile, value, 0, value.length)
//      reader.read().then(writeToPyFS)
//    }
//  }
//  reader.read().then(writeToPyFS)
//}

function initialise () {
  console.log('[ProcessingWorker] initialise')
  return startPyodide().then((pyodide) => {
    self.pyodide = pyodide
    return loadPackages()})
  .then(() => {
    return installPortPackage()
  })
}

function startPyodide() {
  importScripts('https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js')

  console.log('[ProcessingWorker] loading Pyodide')
  return loadPyodide({
    indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.22.1/full/'
  })  
}

function loadPackages() {
  console.log('[ProcessingWorker] loading packages')
  return self.pyodide.loadPackage(['micropip', 'python-magic', 'pandas', 'beautifulsoup4', 'lxml'])
}

function installPortPackage() {
  console.log('[ProcessingWorker] load port package')
  return self.pyodide.runPythonAsync(`
    import micropip
    await micropip.install("/port-0.0.0-py3-none-any.whl", deps=False)

    import port
  `);  
}

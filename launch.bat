taskkill /FI "WINDOWTITLE eq pylar-*"
start cmd /k pylar-broker -l tcp://0.0.0.0:3333
start cmd /k pylar-service arithmetic -c tcp://127.0.0.1:3333
start cmd /k pylar-broker -l tcp://0.0.0.0:3334
start cmd /k pylar-service authentication -c tcp://127.0.0.1:3334
start cmd /k pylar-iservice link -c tcp://127.0.0.1:3333 -c tcp://127.0.0.1:3334
start cmd /k pylar-iservice link -c tcp://127.0.0.1:3333 -c tcp://127.0.0.1:3334

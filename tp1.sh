
BuildTp(){

  if [ "$1" == "server" ]; then
      docker build -f ./server/Dockerfile -t "server:latest" .

  elif [ "$1" == "client" ]; then
      docker build -f ./client/Dockerfile -t "client:latest" .

  else
      docker build -f ./server/Dockerfile -t "server:latest" .
      docker build -f ./client/Dockerfile -t "client:latest" .
  fi
}

StopTp(){
    
    docker compose -f docker-compose-gen.yaml stop -t 5

    if [ "$1" == "-k" ]; then
    docker compose -f docker-compose-gen.yaml down --remove-orphans
    fi

}   

RunTp(){

    if [ "$1" == "test" ]; then

        # Temporary container to test the server
        docker build -f ./netcat/Dockerfile -t "netcat:latest" .
        docker run --rm --network "$(cat ./server/network)" -i -t "netcat:latest"

    elif [ "$1" == "logs" ]; then

        # Show logs
        docker compose -f docker-compose-gen.yaml logs -f

    else

        # Docker Compose Up
        python3 composer.py --clients "$1"
        docker compose -f docker-compose-gen.yaml up -d --build

    fi

}

myfun(){
    echo $1 $2 $3 $4
}

if [ "$1" == "build" ]; then
    shift
    BuildTp $@

elif [ "$1" == "run" ]; then
    shift
    RunTp $@

elif [ "$1" == "stop" ]; then
    shift
    StopTp $@

elif [ "$1" == "help" ]; then

    echo ""
    echo "  - run [test|logs]   Inicia la arquitectura definida en 'composer.py'"
    echo "                           test: Instancia de netcat conectada a la misma red"
    echo "                           logs: Muestra los logs"
    echo ""
    echo "  - stop [-k]          Detiene los contenedores corriendo"
    echo "                           -k: elimina los contenedores"
    echo ""

else
    echo "Comando desconocido.."
    echo "Prueba con $0 help"
fi



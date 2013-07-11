package edu.zookeeper;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.zookeeper.*;

public class SimpleLeader implements Watcher {

    public static void main(String args[]) throws Exception {
		int clientesBrigandoPorLideranca = 10;
		CountDownLatch countDown = new CountDownLatch(clientesBrigandoPorLideranca);
		for (int contador = 0; contador < clientesBrigandoPorLideranca; contador++)
			new ExecutorLideranca(contador, countDown).start();		
	}

    private static final int TEMPO_MAXIMO_VIDA_MILLSECS = 20000;
	
	private final int posicao;
	private final int porta;
	private final ZooKeeper zk;
	
	private String candidatura;
    private boolean lider;
	
	public SimpleLeader(int posicao, ZooKeeper zk) throws Exception {
        this.zk = zk;
        this.posicao = posicao;
        this.porta = 9000 + posicao;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() != Watcher.Event.EventType.None){
            System.out.println(posicao + " - O caminho [" + event.getPath() + 
                    "] foi alterado. Tipo de alteração [" + event.getType() + "]");
            validarLideranca();
        }
    }

	private void validarLideranca() {
		try{
			// Validando se ganhei a liderança
			List<String> candidatos = zk.getChildren(Constantes.CAMINHO_ELEICAO, null);
			Collections.sort(candidatos);
			String ganhador = candidatos.get(0);
			if (candidatura.endsWith(ganhador)) {
				System.out.println(posicao + " - [" + candidatura + " ] Sou o lider");
				this.lider = true;
				registraEndereco();
			} else {
				System.out.println(posicao + " - [" + candidatura + " ] Não sou o lider, ");
				String concorrente = "";
				for (int indice = 0; indice < candidatos.size(); indice++) {
					String atual = candidatos.get(indice);
					if (this.candidatura.endsWith(atual)) {
						concorrente = candidatos.get(indice - 1);
						System.out.println(posicao + " - perdi para o candidato " + concorrente);
						break;
					}
				}
				
				// Observando se o lider anterior ainda está vivo.
				zk.exists(Constantes.CAMINHO_ELEICAO + "/" + concorrente, this);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

    private void registraEndereco() throws KeeperException, InterruptedException {
        String enderecoServidor = "localhost:" + porta;
        
        System.out.println("Registrando endereço: " + enderecoServidor);
        zk.create(Constantes.CAMINHO_ATUAL, enderecoServidor.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
    
    /**
     * Classe interna para esperar a criação de várias threads e executar o serviço.
     */
    private static class ExecutorLideranca extends Thread {

        private CountDownLatch countDown;
        private int indice;

        public ExecutorLideranca(int indice, CountDownLatch countDown) throws IOException {
            this.indice = indice;
            this.setDaemon(false);
            this.countDown = countDown;
        }

        @Override
        public void run() {
            try {
                // Aguardando as outras threas
                countDown.countDown();
                countDown.await();
                ZooKeeper zookeeper = null;
                try  {
                    zookeeper = new ZooKeeper("localhost:2181", 10000, null);
                    
                    SimpleLeader simpleLeader = new SimpleLeader(indice, zookeeper);
                    simpleLeader.service();
                }finally{
                    if (zookeeper != null)
                        zookeeper.close();
                }
                
            } catch (Exception ex) {
                throw new RuntimeException("Erro iniciando o cliente" , ex);
            }
        }
    }

    public void service() throws Exception {
        CaminhoZookeeper.criaSeNaoExiste(Constantes.CAMINHO_ELEICAO, zk);
        CaminhoZookeeper.criaSeNaoExiste(Constantes.CAMINHO_CONFIGURACAO, zk);
        
        try {
            candidatura = zk.create(Constantes.CAMINHO_ELEICAO + "/candidato-", Constantes.NENHUM_CONTEUDO, 
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            validarLideranca();
            
            Random random = new Random();   
            int tempoDeEspera = random.nextInt(TEMPO_MAXIMO_VIDA_MILLSECS);
            System.out.println(posicao + " - Esperando conexão por " + (tempoDeEspera/1000) + " segundos");
            
            AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(porta));
            
            while(tempoDeEspera > 0){
                try{
                    long inicioDeEspera = System.currentTimeMillis();
                    
                    Future<AsynchronousSocketChannel> clienteFuturo = server.accept();
                    AsynchronousSocketChannel client = clienteFuturo.get(tempoDeEspera, TimeUnit.MILLISECONDS);
                    
                    long duracao = System.currentTimeMillis() - inicioDeEspera;
                    tempoDeEspera -= duracao;
                    
                    inicioDeEspera = System.currentTimeMillis();
                    String mensagem =  this.lider ? 
                            "Ola do lider " + posicao + " viverei por mais " + (tempoDeEspera/1000) + "s\n": 
                            "Não deveria estar falando comigo\n";
                        
                    ByteBuffer buffer = ByteBuffer.wrap(mensagem.getBytes());
                    client.write(buffer).get();
                    client.close();
                    
                    duracao = System.currentTimeMillis() - inicioDeEspera;
                    tempoDeEspera -= duracao;
                    
                }catch(TimeoutException ex){
                    tempoDeEspera = 0;
                }
            }
            server.close();
            
            
            
        } catch (Exception ex) {
            throw new RuntimeException(posicao + " - Erro tentando liderança", ex);
        }
    }
}

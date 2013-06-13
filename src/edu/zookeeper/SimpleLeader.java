package edu.zookeeper;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.*;

public class SimpleLeader implements Watcher {

	public static void main(String args[]) throws Exception {
		ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, null);
		int clientesBrigandoPorLideranca = 13;
		CountDownLatch countDown = new CountDownLatch(
				clientesBrigandoPorLideranca);
		for (int contador = 0; contador < clientesBrigandoPorLideranca; contador++)
			new ExecutorLideranca(contador, zk, countDown).start();
	}

	private static byte[] nenhumConteudo = "".getBytes();
	private static final String caminhoDaEleicao = "/sistema/eleicao";
	private final String candidatura;
	private boolean lider;
	private int posicao;
	private ZooKeeper zk;

	private static class ExecutorLideranca extends Thread {

		private ZooKeeper zk;
		private CountDownLatch countDown;
		private int indice;

		public ExecutorLideranca(int indice, ZooKeeper zk,
				CountDownLatch countDown) {
			this.indice = indice;
			this.zk = zk;
			this.countDown = countDown;
		}

		@Override
		public void run() {
			try {
				// Aguardando as outras threas
				countDown.countDown();
				countDown.await();

				new SimpleLeader(indice, zk);
			} catch (Exception ex) {

			}
		}
	}

	public SimpleLeader(int posicao, ZooKeeper zk) {
		this.posicao = posicao;
		this.zk = zk;
		try {
			candidatura = zk.create(caminhoDaEleicao + "/candidato", nenhumConteudo, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			validarLideranca();
			
			Thread.sleep(10000);

			zk.close();
		} catch (Exception ex) {
			throw new RuntimeException("Erro tentando liderança", ex);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		if (event.getType() != Watcher.Event.EventType.None)
			validarLideranca();
	}

	private void validarLideranca() {
		try{
			// Validando se ganhei a liderança
			List<String> candidatos = zk.getChildren(caminhoDaEleicao, null);
			Collections.sort(candidatos);
			String ganhador = candidatos.get(0);
			if (candidatura.endsWith(ganhador)) {
				System.out.println(posicao + " [" + candidatura + " ] Sou o lider");
				lider = true;
			} else {
	
				System.out.print(posicao + " [" + candidatura + " ] Não sou o lider, ");
				String concorrente = "";
				for (int indice = 0; indice < candidatos.size(); indice++) {
					String atual = candidatos.get(indice);
					if (this.candidatura.endsWith(atual)) {
						concorrente = candidatos.get(indice - 1);
						System.out.println("perdi para o candidato " + concorrente);
						break;
					}
				}
	
				// Obsercando se o lider anterior ainda está vivo.
				zk.exists(caminhoDaEleicao + "/" + concorrente, this);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}
}

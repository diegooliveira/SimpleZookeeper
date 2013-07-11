package edu.zookeeper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class SimpleClientWatch implements Watcher {

    public static void main(String args[]) throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 10000, null);
        SimpleClientWatch simpleClientWatch = new SimpleClientWatch(zk);
        simpleClientWatch.executa();
    }

    private final ZooKeeper zk;
    private volatile String endereco;

    public SimpleClientWatch(ZooKeeper zk) {
        this.zk = zk;
    }

    private void executa() throws Exception {
        carregaEndereco();
        CaminhoZookeeper.criaSeNaoExiste(Constantes.CAMINHO_CONFIGURACAO, zk);
        while (true) {
            if (this.endereco != null)
                lerMensagem(this.endereco);
            
            Thread.sleep(1000);
        }
    }

    private void lerMensagem(String enderecoAtual) {
        String[] partes = enderecoAtual.split(":");
        try (Socket socket = new Socket(partes[0], Integer.parseInt(partes[1]));
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String mensagemDoServidor = reader.readLine();
            System.out.println("Recebi: \"" + mensagemDoServidor + "\"");
        }catch (Exception e) {
            System.out.println("Não consegui conectar com " + enderecoAtual);
        }
    }

    private void carregaEndereco() {
        Stat stat;
        try {
            stat = zk.exists(Constantes.CAMINHO_ATUAL, this);
            if (stat != null) {
                
                byte[] dados = zk.getData(Constantes.CAMINHO_ATUAL, false, stat);
                this.endereco = new String(dados);
                
                System.out.println("Novo endereço do servidor: " + this.endereco);
            }else {
                System.out.println("Não existe configuração");
                zk.getChildren(Constantes.CAMINHO_CONFIGURACAO, this);
            }
            
        } catch (KeeperException | InterruptedException e) {
            this.endereco = null;
        }

    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() != Watcher.Event.EventType.None) {
            System.out.println("Alguma coisa mudou");
            this.endereco = null;
            carregaEndereco();
        }
    }

}

package edu.zookeeper;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * Exemplo simples de código que usa o Apache Zookeeper para baixar de páginas e
 * imagens.
 */
public class ExemploFila {

    public static void main(String args[]) throws Exception {
        // Preparando a conexão com o zookeeper
        ZooKeeper zookeeper = new ZooKeeper("localhost", TIME_OUT_10_SEGUNDOS, null);
        Path caminhoASalvar = Files.createTempDirectory("download");

        // Criando um baixador que ira adicionar as páginas e baixar o conteúdo.
        List<String> paginas = Arrays.asList("http://www.uol.com.br", "http://www.globo.com");
        BaixadorConteudo baixadorConteudo = new BaixadorConteudo(zookeeper, caminhoASalvar, paginas);
        baixadorConteudo.setDaemon(false);
        baixadorConteudo.start();

        // Iniciando os baixadores
        for (int i = 1; i < 10; i++)
            new BaixadorConteudo(zookeeper, caminhoASalvar).start();
    }

    /**
     * Classe responsável por baixar o conteúdo.
     */
    public static class BaixadorConteudo extends Thread {

        private ZooKeeper zookeeper;
        private List<String> paginas;
        private Path pastaSalvarConteudo;

        private Pattern padraoImageNaPagina;

        public BaixadorConteudo(ZooKeeper zookeeper, Path pastaSalvarConteudo) {
            this(zookeeper, pastaSalvarConteudo, Collections.<String> emptyList());
        }

        public BaixadorConteudo(ZooKeeper zookeeper, Path pastaSalvarConteudo, List<String> paginas) {
            this.zookeeper = zookeeper;
            this.pastaSalvarConteudo = pastaSalvarConteudo;
            this.paginas = paginas;
            this.padraoImageNaPagina = Pattern.compile("<img\\s*.*src=\"(?<endereco>.*?)\"");
        }

        @Override
        public void run() {

            System.out.println("Iniciando execução");
            try {

                String caminhoFilaServidor = criaCaminhoFilaServidor();
                // Adicionando as páginas para serem baixadas
                for (String endereco : paginas)
                    adiciona(caminhoFilaServidor, "pagina", endereco);

                // Processando os trabalhos da fila
                while (!isInterrupted()) {

                    // Tentando adquirir um novo trabalho
                    Observador observador = new Observador();
                    List<String> trabalhos = zookeeper.getChildren(caminhoFilaServidor, observador);
                    if (trabalhos.isEmpty()) {
                        
                        // Não há nada a fazer, vamos aguardar que algum trabalho novo apareça
                        // para tentar novamente
                        observador.espere();
                        continue;
                    }

                    // Tentando processar os trabalhos listados
                    for (String trabalho : trabalhos) {
                        try {
                            String caminhoTrabalho = caminhoFilaServidor + "/" + trabalho;

                            String descricaoTrabalho = new String(zookeeper.getData(caminhoTrabalho, false, null));
                            zookeeper.delete(caminhoTrabalho, -1);

                            String[] detalhes = descricaoTrabalho.split("\n");

                            String tipo = detalhes[0];
                            String endereco = detalhes[1];

                            byte[] conteudo = executaDownload(endereco);
                            salvaConteudo(endereco, conteudo);

                            switch (tipo) {
                            case "pagina":
                                processaPagina(caminhoFilaServidor, endereco, conteudo);
                                System.out.println("Página salva " + endereco);
                                break;
                            case "imagem":
                                System.out.println("Imagem salva " + endereco);
                                break;
                            default:
                                break;
                            }
                        } catch (KeeperException.NoNodeException e) {
                            // Alguém já pegou esse trabalho.
                        }
                    }

                }
            } catch (Exception ex) {
                throw new RuntimeException("Erro fazendo download", ex);
            }
        }

        private String criaCaminhoFilaServidor() {
            System.out.println("Validando caminho de fila");
            try {
                criaZnode("/sistema");
                criaZnode("/sistema/download");

                return "/sistema/download";
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException("Erro criando o caminho da fila no zookeeper", e);
            }
        }

        private void criaZnode(String path) throws KeeperException, InterruptedException {
            Stat existe = zookeeper.exists(path, false);
            if (existe == null)
                zookeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        /**
         * Método que salva o conteúdo no pasta de destino
         * 
         * @param endereco
         *            O endereço que foi baixada
         * @param conteudo
         *            O conteúdo que deve ser salvo
         */
        private void salvaConteudo(String endereco, byte[] conteudo) {
            try {
                String enderecoLimpo = endereco.replace("http://", "").replace(":", "").replace("?", "");
                Path caminho = pastaSalvarConteudo.resolve(enderecoLimpo);
                Path pastaConteudo = caminho.getParent();
                Files.createDirectories(pastaConteudo);
                
                System.out.println("Salvando conteúdo " + endereco + " em " + caminho);
                Files.write(caminho, conteudo, StandardOpenOption.CREATE);
            } catch (Exception ex) {
                throw new RuntimeException("Erro salvando conteudo", ex);
            }
        }

        /**
         * Salva o conteúdo do HTML e processa todas as imagens adicionando-as
         * na fila.
         * 
         * @param endereco
         *            O endereço da página que foi baixada.
         * @param conteudo
         *            O conteúdo HTML.
         */
        private void processaPagina(String caminho, String endereco, byte[] conteudo) {
            System.out.println("Processando página " + endereco);
            try {
                String html = new String(conteudo);
                Matcher matcher = padraoImageNaPagina.matcher(html);
                while (matcher.find()) {
                    String enderecoImagem = matcher.group("endereco");
                    adiciona(caminho, "imagem", enderecoImagem);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Erro procesando página", ex);
            }
        }

        private byte[] executaDownload(String endereco) {
            System.out.println("Baixando " + endereco);
            try {
                URL url = new URL(endereco);
                URLConnection conexao = url.openConnection();
                try (InputStream entrada = conexao.getInputStream();
                        ByteArrayOutputStream saida = new ByteArrayOutputStream()) {
                    byte[] buffer = new byte[1024];
                    int lido;
                    while ((lido = entrada.read(buffer)) > 0) {
                        saida.write(buffer, 0, lido);
                    }
                    return saida.toByteArray();
                }
            } catch (Exception ex) {
                throw new RuntimeException("Erro baixando o conteúdo de [" + endereco + "]", ex);
            }
        }

        private void adiciona(String caminho, String tipo, String endereco) throws Exception {
            System.out.println("Adicionando " + tipo + " " + endereco);
            StringBuilder sb = new StringBuilder();
            sb.append(tipo).append("\n");
            sb.append(endereco);
            zookeeper.create(caminho + "/trabalho-", sb.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
        }
    }

    private static class Observador implements Watcher {

        CountDownLatch contador;

        public Observador() {
            contador = new CountDownLatch(1);
        }

        @Override
        public void process(WatchedEvent event) {
            contador.countDown();
        }

        public void espere() throws InterruptedException {
            contador.await();
        }

    }

    private static final int TIME_OUT_10_SEGUNDOS = 10000;

}

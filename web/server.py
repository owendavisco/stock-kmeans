import string,cgi,time
from os import curdir, sep
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer


class MyHandler(BaseHTTPRequestHandler):

    def write_header(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()


    def do_GET(self):
        try:
            if self.path == "/":
                self.write_header()
                f = open(curdir + sep + "index.html", "rd")
                self.wfile.write(f.read())
                f.close()
                return

            elif self.path.endswith(".html") or self.path.endswith(".json"):
                self.write_header()
                f = open(curdir + sep + self.path, "rd")
                self.wfile.write(f.read())
                f.close()
                return

        except IOError:
            self.send_error(404,'File Not Found: %s' % self.path)


if __name__ == '__main__':
    try:
        server = HTTPServer(('', 8080), MyHandler)
        print 'started httpserver...'
        server.serve_forever()
    except KeyboardInterrupt:
        print '^C received, shutting down server'
        server.socket.close()
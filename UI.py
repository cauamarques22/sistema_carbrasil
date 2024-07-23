import customtkinter as ctk
class Display:

    def __init__(self):
        self.root = ctk.CTk()
        self.root.geometry("1366x760")
        self.root.title("Sistema Integrador")
        ctk.set_default_color_theme("blue")
        ctk.set_appearance_mode("dark")
        

        self.frames()
        self.widgets()
        self.current_frame = self.app_frame1
        self.root.iconbitmap("logo_cb.ico")
    def frames(self):

        self.menu_frame = ctk.CTkFrame(self.root, bg_color="#282828", fg_color="#282828")
        self.menu_frame.place(relx=0, rely=0, relwidth=0.15, relheight=1)

        #Frame 1
        self.app_frame1 = ctk.CTkFrame(self.root, fg_color="#141414")
        self.app_frame1.place(relx=0.15, rely=0, relwidth=0.85, relheight=1)

        self.app_subframe1 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe1.place(relx=0, rely=0, relwidth=0.5, relheight=0.5)

        self.app_subframe2 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe2.place(relx=0.5, rely=0, relwidth=0.5, relheight=0.5)

        self.app_subframe3 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe3.place(relx=0, rely=0.5, relwidth=0.5, relheight=0.5)

        self.app_subframe4 = ctk.CTkFrame(self.app_frame1, fg_color="#141414")
        self.app_subframe4.place(relx=0.5, rely=0.5, relwidth=0.5, relheight=0.5)

        #Frame 2
        self.app_frame2 = ctk.CTkFrame(self.root, bg_color="#141414", fg_color="#141414")

        self.app2_subframe = ctk.CTkFrame(self.app_frame2, fg_color="#141414")
        self.app2_subframe.place(relx=0, rely=0, relwidth=0.5, relheight=0.5)

    def widgets(self):
        #Widgets Modulo 1
        self.modulo1_label = ctk.CTkLabel(self.app_subframe1,text="Modulo 1", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo1_label.place(relx=0.02, rely=0.04)

        self.modulo1_textbox = ctk.CTkTextbox(self.app_subframe1, width=0, height=0, font=("Montserrat", 14),state="disabled")
        self.modulo1_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 2
        self.modulo2_label = ctk.CTkLabel(self.app_subframe2,text="Modulo 2", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo2_label.place(relx=0.02, rely=0.04)

        self.modulo2_textbox = ctk.CTkTextbox(self.app_subframe2, width=0, height=0, font=("Montserrat", 14), state="disabled")
        self.modulo2_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 3
        self.modulo3_label = ctk.CTkLabel(self.app_subframe3,text="Modulo 3", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo3_label.place(relx=0.02, rely=0.04)

        self.modulo3_textbox = ctk.CTkTextbox(self.app_subframe3, width=0, height=0, font=("Montserrat", 14),state="disabled")
        self.modulo3_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Modulo 4
        self.modulo4_label = ctk.CTkLabel(self.app_subframe4,text="Modulo 4", font=("Montserrat", 18, "bold"), text_color="red")
        self.modulo4_label.place(relx=0.02, rely=0.04)

        self.modulo4_textbox = ctk.CTkTextbox(self.app_subframe4, width=0, height=0, font=("Montserrat", 14),state="disabled")
        self.modulo4_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Widgets Frame 2
        self.error_label = ctk.CTkLabel(self.app2_subframe,text="Mensagens de Erro", font=("Montserrat", 18, "bold"))
        self.error_label.place(relx=0.02, rely=0.04)

        self.error_textbox = ctk.CTkTextbox(self.app2_subframe, width=0, height=0, font=("Montserrat", 14),state="disabled")
        self.error_textbox.place(relx=0.02, rely=0.16,relwidth=0.96, relheight=0.78)

        #Menu Buttons
        std_x = 0.05
        std_width = 0.9
        self.start_btn = ctk.CTkButton(self.menu_frame, text="Iniciar Sincronização",font=("Montserrat", 14, "bold") ,command=self.start)
        self.start_btn.place(relx=std_x, relwidth=std_width, rely=0.02, relheight=0.05)

        self.pause_btn = ctk.CTkButton(self.menu_frame, text="Pausar Sincronização",font=("Montserrat", 14, "bold"), command=self.pause_thread, state="disabled")
        self.pause_btn.place(relx=std_x, relwidth=std_width, rely=0.09, relheight=0.05)

        self.continue_btn = ctk.CTkButton(self.menu_frame, text="Continuar Sincronização", font=("Montserrat", 14, "bold"),command=self.continuar_thread, state="disabled")
        self.continue_btn.place(relx=std_x, relwidth=std_width, rely=0.16, relheight=0.05)

        self.stop_btn = ctk.CTkButton(self.menu_frame, text="Parar Sincronização",font=("Montserrat", 14, "bold"), command=self.parar_thread, state="disabled")
        self.stop_btn.place(relx=std_x, relwidth=std_width, rely=0.23, relheight=0.05)

        self.f1_btn = ctk.CTkButton(self.menu_frame, text="Painel Principal",font=("Montserrat", 14, "bold"), command=lambda: self.switch_frame(self.app_frame1))
        self.f1_btn.place(relx=std_x, relwidth=std_width, rely= 0.5, relheight=0.1)

        self.f2_btn = ctk.CTkButton(self.menu_frame, text="Painel de Informações",font=("Montserrat", 14, "bold"), command=lambda: self.switch_frame(self.app_frame2))
        self.f2_btn.place(relx=std_x, relwidth=std_width, rely= 0.62, relheight=0.1)

        self.continue_btn.bind("<Configure>", self.resize_text)

        # self.f3_btn = ctk.CTkButton(self.menu_frame, text="Get TxtBox", command= self.get_txtbox)
        # self.f3_btn.place(relx=0.2, relwidth=0.6, rely= 0.8, relheight=0.1)

class UIFunctions(Display):
    def __init__(self):
        super().__init__() 

    def switch_frame(self,target_frame):
        self.current_frame.place_forget()
        target_frame.place(relx=0.15, rely=0, relwidth=0.85, relheight=1)
        self.current_frame = target_frame

    def get_txtbox(self):
        texto = self.modulo1_textbox.get("1.0", ctk.END)
        self.modulo2_textbox.insert(ctk.END, texto)
    
    def resize_text(self, event):
    # Ajusta o comprimento da quebra de linha com base na largura do botão
        wrap_length = event.width  # Ajuste conforme necessário
        self.continue_btn._text_label.configure(wraplength=wrap_length)
